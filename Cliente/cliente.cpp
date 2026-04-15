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

// função para adição das chamadas rpc
#include "rpc/server.h"
#include "rpc/client.h"

// abrevisações de funções do C++
namespace fs = std::filesystem;

//definições de constantes utilizadas no códigos
#define download 1
#define upload 2
#define privado 3
#define somente_leitura 4
#define escrita 5

// estrutura de dados para salvar os metadados dos arquivos relacionados ao cliente
struct EstadoLocal {
    int versao;
    std::filesystem::file_time_type ultima_modificacao;
    int permissao;
};

//variáveis de uso global
int minha_porta;
std::string meu_usuario;
std::string meu_diretorio;
std::string meu_ip_atual;
std::mutex acesso_tabela; // variável de semáforo de acesso a tabela de arquivos globais
std::map<std::string, EstadoLocal> meus_arquivos_estados;
rpc::client tracker("186.217.124.198", 8000); // bind do peer tracker

// função adiquirida por IA para obtenção do IP do peer por meio de comunicação ao servidor do google
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

//função que faz o download de acordo com os pares de ip e porta devolvida pelo servidor tracker, futuramente adicionar lógica de partição de arquivo para fazer o download de várias fontes
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

//função de sincronização automática dos arquivos baixados e compartilhados, também realiza a verificação de ip para que o código também permita a mobilidade e transição do computador com mudanças de IP no meio do programa
void sincronizador_automatico() {
   
    while (true) {
        // a função roda sempre a cada 5 segundos
        std::this_thread::sleep_for(std::chrono::seconds(5));

        try {
            std::string ip_verificado = obter_meu_ip();
            if (ip_verificado != meu_ip_atual) {
                tracker.call("atualizar_ip_usuario", meu_usuario, ip_verificado);
                meu_ip_atual = ip_verificado;
            }
        } catch (...) {}

        //passa por todos arquivos pertencentes ao diretório do atual usuário logado
        for (const auto& entrada : fs::directory_iterator(meu_diretorio)) {
            if (entrada.is_regular_file()) {
                std::string nome_arq = entrada.path().filename().string();

                //elimina tipos de arquivos especiais no qual podem ser gerados
                if (nome_arq.front() == '.' || nome_arq.back() == '~') {
                    continue; 
                }

                //obtem o tempo atual do arquivo através do seu metadado
                auto tempo_atual = std::filesystem::last_write_time(entrada.path());

                
                bool arquivo_adicionado = false;
                bool foi_modificado = false;
                bool verificar_rede = false;
                int versao_local = 0;

                { // pede acesso para fazer leituras na tabela de dados e realiza todas as operações
                    std::lock_guard<std::mutex> lock_leitura(acesso_tabela);
                    
                    if (meus_arquivos_estados.find(nome_arq) == meus_arquivos_estados.end()) {
                        arquivo_adicionado = true;
                    } else if (tempo_atual > meus_arquivos_estados[nome_arq].ultima_modificacao) {
                        foi_modificado = true;
                    } else {
                        verificar_rede = true;
                        versao_local = meus_arquivos_estados[nome_arq].versao;
                    }
                } // Aqui o lock_leitura morre e a tabela fica livre

                //ações que podem ser tomadas de acordo com as verificações realizadas acima
                try {
                    if (arquivo_adicionado) {
                        std::cout << "\n[AUTO-SYNC] Novo arquivo detectado localmente: " << nome_arq << "\n";
                        int nova_v = tracker.call("registrar_peer", meu_usuario, nome_arq, privado, upload).as<int>();
                        
                        //escrita rápida
                        std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                        meus_arquivos_estados[nome_arq] = {nova_v, tempo_atual, privado};
                    } 
                    else if (foi_modificado) {
                        std::cout << "\n[AUTO-SYNC] Mudanca local em " << nome_arq << " detectada!\n";
                        int nova_v = tracker.call("atualizar_arquivo", meu_usuario, nome_arq).as<int>();
                        
                        if (nova_v == -2) {
                            std::cout << "[AUTO-SYNC] BLOQUEADO: Arquivo somente leitura ou privado!\n";
                            fazer_download(nome_arq, tracker);
                            
                            //o arquivo coloca o tempo atual como o estado mais atual do arquivo mesmo sendo o mesmo
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

// Função que recebe a requisição de outros nós e transferem os dados para o nó requirinte
std::vector<char> transferir_arquivo(std::string nome_arquivo) {
    //obtem o endereço até o arquivo requirido
    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    std::cout << "[PEER SERVIDOR] Pedido de download recebido para: " << nome_arquivo << "\n";

    //ponteiro do arquivo
    std::ifstream arquivo(caminho_completo, std::ios::binary | std::ios::ate);
    if (!arquivo.is_open()) return std::vector<char>(); // se arquivo não abriu retorna sem conteúdo

    //obtem o tamanho do conteúdo contido no arquivo
    std::streamsize tamanho = arquivo.tellg();
    arquivo.seekg(0, std::ios::beg);
    std::vector<char> buffer(tamanho);

    // faz a leitura de dados
    if (arquivo.read(buffer.data(), tamanho)) {
        return buffer;
    }

    //retorna os dados lidos
    return std::vector<char>();
}

// menu de cadastramento de arquivos, futuramente será um modificador de permissão de arquivo ou exlusão
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
    } while (permissao < 0 || permissao > 2); 
    
    permissao += 3;

    std::cout << "%d" << permissao;

    int estado = tracker.call("registrar_peer", meu_usuario, arquivo, permissao, upload).as<int>();
    
    if (estado == 1) {
        std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
        std::cout << "Arquivo registrado com sucesso!\n";
        
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

// Menu de criação de arquivos e setador de permissões
void criar_arquivo_txt() {
    std::string nome_arquivo, conteudo;
    int permissao;

    // Obtenho todos os dados do arquivo para depois cria-lo, faço isso por condição de corrida ocorrida pelo sincronizador de arquivos, no qual antes dessa mudança as permições eram setadas por aquela função ao invés do usuário
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
                
                meus_arquivos_estados[nome_arquivo] = {1, std::filesystem::last_write_time(caminho_completo), permissao};
            }
        }
 }

// Função que gera um arquivo base de testes, futuramente será removido
void criar_arquivo_aleatorio(std::string nome_arquivo) {
    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    std::ofstream arquivo(caminho_completo);
    if (arquivo.is_open()) {
        arquivo << "Este arquivo pertence a " << meu_usuario << "\n";
        arquivo << "Chave: " << rand() % 10000 << "\n";
        arquivo.close();
    }
}

// função que realiza a leitura inicial do sistema de arquivos com base no usuário, realiza a leitura dos arquivos da pasta do usuário atual ou cria uma nova pasta se o usuário nunca acessou pelo computador. Modificações de instalação de arquivos nos quais pertencem ao usuário mas não estão no computador atual pode ser uma boa implementação
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

    // configuração de execução assincrona da única porta de recepção de dados do peer cliente, futuras implementações podem exigir porta fixa por conta do uso de mais de uma porta para a verificação de peers mais recomendados para fazer o download
    rpc::server srv(minha_porta);
    srv.bind("transferir_arquivo", &transferir_arquivo);
    srv.async_run(1); 
    std::cout << "[PEER] Escutando pedidos na porta " << minha_porta << "...\n";

    std::thread thread_sync(sincronizador_automatico); // cria uma thread para o sincronizador rodar em paralelo com o sistema do usuário sem atrapalhar a sua experiência
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