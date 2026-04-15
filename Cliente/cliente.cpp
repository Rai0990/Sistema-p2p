#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <exception>
#include <filesystem>
#include <fstream>
#include <limits>
#include <thread>
#include <chrono>
#include <utility>
#include <mutex>

// Bibliotecas Nativas de Socket do Linux para descobrir o IP
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

#include "rpc/server.h"
#include "rpc/client.h"

namespace fs = std::filesystem;

#define download 1
#define upload 2
#define privado 3
#define somente_leitura 4
#define escrita 5

// --- VARIÁVEIS GLOBAIS ---
int minha_porta;
std::string meu_usuario;
std::string meu_diretorio;
std::string meu_ip_atual;
std::vector<std::string> meus_arquivos; // Histórico legado
std::mutex acesso_tabela;

struct EstadoLocal {
    int versao;
    std::filesystem::file_time_type ultima_modificacao;
    int permissao;
};

std::map<std::string, EstadoLocal> meus_arquivos_estados;
rpc::client tracker("186.217.124.198", 8000); // Tracker Global para o Menu

// ========================================================
// 1. DESCOBERTA DE REDE (IP DINÂMICO)
// ========================================================

// Abre um socket cego para o DNS do Google só para o Linux nos contar nosso próprio IP
std::string obter_meu_ip() {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) return "127.0.0.1";

    struct sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr("8.8.8.8"); 
    serv.sin_port = htons(53);

    // connect num socket UDP não envia dados, só resolve a rota
    int err = connect(sock, (const struct sockaddr*)&serv, sizeof(serv));
    if (err < 0) {
        close(sock);
        return "127.0.0.1";
    }

    struct sockaddr_in name;
    socklen_t namelen = sizeof(name);
    err = getsockname(sock, (struct sockaddr*)&name, &namelen);
    if (err < 0) {
        close(sock);
        return "127.0.0.1";
    }

    const char* p = inet_ntoa(name.sin_addr);
    std::string ip_descoberto(p);
    close(sock);
    
    return ip_descoberto;
}

// ========================================================
// 2. LÓGICA DE DOWNLOAD
// ========================================================

void fazer_download(std::string nome_arq, rpc::client& tracker_ativo) {
    std::string caminho_completo = meu_diretorio + "/" + nome_arq;
    
    using Endereco = std::pair<std::string, int>;
    auto enderecos = tracker_ativo.call("buscar_peers", meu_usuario, nome_arq).as<std::vector<Endereco>>();

    if (!enderecos.empty()) {
        std::string ip_alvo = enderecos[0].first;
        int porta_alvo = enderecos[0].second; 
        
        std::cout << "[PEER] Baixando atualizacao de " << ip_alvo << ":" << porta_alvo << "...\n";
        
        try {
            rpc::client outro_peer(ip_alvo, porta_alvo);
            auto dados_arquivo = outro_peer.call("transferir_arquivo", nome_arq).as<std::vector<char>>();
            
            std::ofstream arquivo_saida(caminho_completo, std::ios::binary); 
            if (arquivo_saida.is_open()) {
                arquivo_saida.write(dados_arquivo.data(), dados_arquivo.size());
                arquivo_saida.close();
                
                std::cout << "[PEER] Download de '" << nome_arq << "' concluido fisicamente!\n";

                tracker_ativo.call("registrar_peer", meu_usuario, nome_arq,NULL, download);
            }
        } catch (const std::exception& e) {
            std::cout << "[ERRO] Falha ao conectar no Peer.\n";
        }
    } else {
         std::cout << "[PEER] O dono do arquivo esta offline. Download pendente.\n";
    }
}

// ========================================================
// 3. O VIGIA (THREAD DE BACKGROUND)
// ========================================================

void sincronizador_automatico() {
   

    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));

        try {
            std::string ip_verificado = obter_meu_ip();
            if (ip_verificado != meu_ip_atual) {
                tracker.call("atualizar_ip_usuario", meu_usuario, ip_verificado);
                meu_ip_atual = ip_verificado;
            }
        } catch (...) {}

        for (const auto& entrada : fs::directory_iterator(meu_diretorio)) {
            if (entrada.is_regular_file()) {
                std::string nome_arq = entrada.path().filename().string();

                if (nome_arq.front() == '.' || nome_arq.back() == '~') {
                    continue; 
                }

                auto tempo_atual = std::filesystem::last_write_time(entrada.path());

                // ===================================================
                // PASSO 1: LEITURA RÁPIDA (Trava e destrava imediatamente)
                // ===================================================
                bool eh_novo = false;
                bool foi_modificado = false;
                bool verificar_rede = false;
                int versao_local = 0;

                { // Abre um mini-escopo só para o Lock de Leitura
                    std::lock_guard<std::mutex> lock_leitura(acesso_tabela);
                    
                    if (meus_arquivos_estados.find(nome_arq) == meus_arquivos_estados.end()) {
                        eh_novo = true;
                    } else if (tempo_atual > meus_arquivos_estados[nome_arq].ultima_modificacao) {
                        foi_modificado = true;
                    } else {
                        verificar_rede = true;
                        versao_local = meus_arquivos_estados[nome_arq].versao;
                    }
                } // Aqui o lock_leitura morre e a tabela fica livre!

                // ===================================================
                // PASSO 2: AÇÃO NA REDE (Livre de Travas!)
                // ===================================================
                try {
                    if (eh_novo) {
                        std::cout << "\n[AUTO-SYNC] Novo arquivo detectado localmente: " << nome_arq << "\n";
                        int nova_v = tracker.call("registrar_peer", meu_usuario, nome_arq, privado, upload).as<int>();
                        
                        // PASSO 3: ESCRITA RÁPIDA
                        std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                        meus_arquivos_estados[nome_arq] = {nova_v, tempo_atual, privado};
                    } 
                    else if (foi_modificado) {
                        std::cout << "\n[AUTO-SYNC] Mudanca local em " << nome_arq << " detectada!\n";
                        int nova_v = tracker.call("atualizar_arquivo", meu_usuario, nome_arq).as<int>();
                        
                        if (nova_v == -2) {
                            std::cout << "[AUTO-SYNC] BLOQUEADO: Arquivo somente leitura ou privado!\n";
                            fazer_download(nome_arq, tracker);
                            
                            std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                            meus_arquivos_estados[nome_arq].ultima_modificacao = std::filesystem::last_write_time(entrada.path());
                        } else {
                            std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                            meus_arquivos_estados[nome_arq].versao = nova_v;
                            meus_arquivos_estados[nome_arq].ultima_modificacao = tempo_atual;
                        }
                    } 
                    else if (verificar_rede) {
                        int versao_rede = tracker.call("verificar_versao", meu_usuario, nome_arq).as<int>();
                        
                        if (versao_rede > versao_local) {
                            std::cout << "\n[AUTO-SYNC] Baixando versao atualizada (v" << versao_rede << ") de " << nome_arq << "...\n";
                            fazer_download(nome_arq, tracker); 

                            auto nova_data_fisica = std::filesystem::last_write_time(entrada.path());
                            
                            std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                            meus_arquivos_estados[nome_arq].versao = versao_rede;
                            meus_arquivos_estados[nome_arq].ultima_modificacao = nova_data_fisica;
                        }
                    }
                } catch (const std::exception& e) {
                    // Ignora falhas de conexão momentâneas
                }
            }
        }
    }
}

// ========================================================
// 4. SERVIDOR DO CLIENTE & FUNÇÕES DO MENU
// ========================================================

std::vector<char> transferir_arquivo(std::string nome_arquivo) {
    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    std::cout << "[PEER SERVIDOR] Pedido de download recebido para: " << nome_arquivo << "\n";

    std::ifstream arquivo(caminho_completo, std::ios::binary | std::ios::ate);
    if (!arquivo.is_open()) return std::vector<char>(); 

    std::streamsize tamanho = arquivo.tellg();
    arquivo.seekg(0, std::ios::beg);
    std::vector<char> buffer(tamanho);

    if (arquivo.read(buffer.data(), tamanho)) {
        return buffer;
    }
    return std::vector<char>();
}

void cadastra_obj() {
    std::string arquivo;
    int permissao;
    std::cout << "Digite o nome do arquivo a ser cadastrado:\n";
    std::cin >> arquivo;

    std::string caminho_completo = meu_diretorio + "/" + arquivo;
    if (!std::filesystem::exists(caminho_completo)) {
        std::cout << "[ERRO] Arquivo nao encontrado na sua pasta local.\n";
        return; 
    }

    do {
        std::cout << "Qual e a permissao deste arquivo para novos usuarios?\n";
        std::cout << "\t 0 - Privado\n\t 1 - Somente leitura \n\t 2 - Leitura e escrita\n";
        std::cin >> permissao;
    } while (permissao < 0 || permissao > 2); // CORREÇÃO: Usar || (OU) em vez de &&
    
    permissao += 3;

    std::cout << "%d" << permissao;

    // Adicionado o parâmetro 'permissao'
    int estado = tracker.call("registrar_peer", meu_usuario, arquivo, permissao, upload).as<int>();
    
    if (estado == 1) {
        std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
        std::cout << "Arquivo registrado com sucesso!\n";
        meus_arquivos.push_back(arquivo);
        
        auto tempo_mod = std::filesystem::last_write_time(caminho_completo);
        meus_arquivos_estados[arquivo] = {1, tempo_mod, permissao};
    }
}

void aquisicao_obj() {
    std::string arquivo_requirido;
    std::cout << "Digite o nome do arquivo desejado:\n";
    std::cin >> arquivo_requirido;

    std::string caminho_completo = meu_diretorio + "/" + arquivo_requirido;
    if (std::filesystem::exists(caminho_completo)) {
        std::cout << "[PEER] Voce ja possui este arquivo salvo no disco!\n";
        return;
    }

    std::cout << "Buscando o arquivo " << arquivo_requirido << " no servidor...\n";
    fazer_download(arquivo_requirido, tracker); 
}

void criar_arquivo_txt() {
    std::string nome_arquivo, conteudo;
    int permissao;

    std::cout << "Nome do arquivo (ex: teste.txt): ";
    std::cin >> nome_arquivo;

    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    std::cout << "Digite o conteudo:\n";
    std::getline(std::cin, conteudo);

    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    
    
   
        

    do {
        std::cout << "Qual e a permissao desse arquivos para novos usuarios?\n";
        std::cout << "\t 0 - Privado\n\t 1 - Somente leitura \n\t 2 - Leitura e escrita\n";
        std::cin >> permissao;
    } while (permissao < 0 || permissao > 2);
        
        permissao += 3;

        if (tracker.call("registrar_peer", meu_usuario, nome_arquivo, permissao, upload).as<int>() == 1) {
            std::cout << "[TRACKER] Arquivo registrado na rede!\n";
            
            // Adiciona no mapa local com segurança
            std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
            std::ofstream arquivo(caminho_completo); 
            if(arquivo.is_open()){
                arquivo << conteudo; 
                arquivo.close();
                std::cout << "[SISTEMA] Arquivo criado no disco!\n";
                meus_arquivos.push_back(nome_arquivo);
                meus_arquivos_estados[nome_arquivo] = {1, std::filesystem::last_write_time(caminho_completo), permissao};
            }
        }
 }


void criar_arquivo_aleatorio(std::string nome_arquivo) {
    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    std::ofstream arquivo(caminho_completo);
    if (arquivo.is_open()) {
        arquivo << "Este arquivo pertence a " << meu_usuario << "\n";
        arquivo << "Chave: " << rand() % 10000 << "\n";
        arquivo.close();
    }
}

void inicializar_sistema_de_arquivos() {
    meu_diretorio = "./pasta_" + meu_usuario;

    if (!fs::exists(meu_diretorio)) {
        fs::create_directory(meu_diretorio);
        criar_arquivo_aleatorio("boas_vindas.txt");
    } 

    std::cout << "[SISTEMA] Sincronizando arquivos locais...\n";
    for (const auto& entrada : fs::directory_iterator(meu_diretorio)) {
        if (entrada.is_regular_file()) { 

            std::string nome_arq = entrada.path().filename().string();
            
            if (nome_arq.front() == '.' || nome_arq.back() == '~') {
                continue; // Pula este arquivo e vai para o próximo do loop
            }

            tracker.call("registrar_peer", meu_usuario, nome_arq, escrita, upload).as<int>();
            int versao = tracker.call("verificar_versao",meu_usuario,nome_arq).as<int>();
            auto tempo_mod = std::filesystem::last_write_time(entrada.path());
            meus_arquivos_estados[nome_arq] = {versao, tempo_mod, privado}; 
            std::cout << " -> [" << nome_arq << "] monitorado.\n";
        }
    }
}

// ========================================================
// 5. MAIN (INICIALIZAÇÃO)
// ========================================================

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "Uso: ./cliente <sua_porta> <nome_usuario>\n";
        return 1;
    }
    
    minha_porta = std::stoi(argv[1]);
    meu_usuario = argv[2]; 
    meu_ip_atual = obter_meu_ip(); // Descobre o IP real nativamente

    std::cout << "[SISTEMA] Inicializando com IP Local: " << meu_ip_atual << "\n";

    // Informa ao servidor o nome, IP real e porta
    tracker.call("registrar_usuario_rede", meu_usuario, meu_ip_atual, minha_porta);

    inicializar_sistema_de_arquivos();

    rpc::server srv(minha_porta);
    srv.bind("transferir_arquivo", &transferir_arquivo);
    srv.async_run(1); 
    std::cout << "[PEER] Escutando pedidos na porta " << minha_porta << "...\n";

    std::thread thread_sync(sincronizador_automatico);
    thread_sync.detach(); 

    int op = 0; 
    while(op != -1){
        std::cout << "\n1: Cadastrar arquivo | 2: Baixar arquivo | 3: Criar arquivo | -1: Sair\nEscolha: ";
        std::cin >> op;

        switch (op) {
            case -1: std::cout << "Encerrando peer...\n"; break;
            case 1: cadastra_obj(); break;
            case 2: aquisicao_obj(); break;
            case 3: criar_arquivo_txt(); break;
            default: std::cout << "Opcao invalida.\n"; break;
        }
    }
    
    return 0; 
}