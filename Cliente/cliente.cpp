#include <iostream>
#include <vector>
#include <string>
#include "rpc/server.h"
#include "rpc/client.h"
#include <exception>
#include <filesystem> // Para ler pastas e verificar arquivos
#include <fstream>    // Para criar e escrever nos arquivos .txt
#include <limits>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

#define download 1
#define upload 2

// --- 2. COMUNICAÇÃO COM O TRACKER (Cliente) ---
rpc::client tracker("127.0.0.1", 8000);
int minha_porta;
std::string meu_usuario;
std::string meu_diretorio;
std::vector<std::string> meus_arquivos;

struct EstadoLocal {
    int versao;
    std::filesystem::file_time_type ultima_modificacao;
};

std::map<std::string, EstadoLocal> meus_arquivos_estados;

void fazer_download(std::string nome_arq){
    std::string caminho_completo = meu_diretorio + "/" + nome_arq;
    auto portas = tracker.call("buscar_peers", minha_porta, nome_arq).as<std::vector<int>>();

    if (!portas.empty()) {
        int porta_alvo = portas[0]; 
        std::cout << "[PEER] Atualizando o arquivo vindo da " << porta_alvo << "\n";
        
        try {
            rpc::client outro_peer("127.0.0.1", porta_alvo);
            auto dados_arquivo = outro_peer.call("transferir_arquivo", nome_arq).as<std::vector<char>>();
            
            // === NOVO: SALVANDO O DOWNLOAD NO DISCO ===
            // std::ios::binary garante que o C++ não corrompa arquivos se não for TXT
            std::ofstream arquivo_saida(caminho_completo, std::ios::binary); 
            
            if (arquivo_saida.is_open()) {
                // Despeja o vetor de bytes recebido direto para dentro do arquivo
                arquivo_saida.write(dados_arquivo.data(), dados_arquivo.size());
                arquivo_saida.close();
                
                std::cout << "[PEER] Download concluido e salvo fisicamente! Recebidos " << dados_arquivo.size() << " bytes.\n";

                // AUTO-SEEDING
                tracker.call("registrar_peer", minha_porta, nome_arq, download);
                meus_arquivos.push_back(nome_arq);
                std::cout << "[PEER] Arquivo registrado. Voce agora e um seeder!\n";
            } else {
                std::cout << "[ERRO] Permissao negada para salvar o arquivo na pasta.\n";
            }

        } catch (const std::exception& e) {
            std::cout << "[ERRO] Falha ao conectar no Peer da porta " << porta_alvo << "\n";
            std::cout << "Motivo técnico: " << e.what() << "\n";
        }
    }
}

void sincronizador_automatico() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 1. VARRE A PASTA FÍSICA PARA ACHAR MUDANÇAS LOCAIS
        for (const auto& entrada : fs::directory_iterator(meu_diretorio)) {
            if (entrada.is_regular_file()) {
                std::string nome_arq = entrada.path().filename().string();
                auto tempo_atual = std::filesystem::last_write_time(entrada.path());

                // Se o arquivo NÃO EXISTE no nosso mapa local, é um arquivo novo!
                if (meus_arquivos_estados.find(nome_arq) == meus_arquivos_estados.end()) {
                    std::cout << "\n[AUTO-SYNC] Novo arquivo detetado localmente: " << nome_arq << "\n";
                    
                    // Registra no Tracker como um upload normal
                    int nova_v = tracker.call("registrar_peer", minha_porta, nome_arq, upload).as<int>();
                    
                    // Adiciona ao nosso mapa de monitoramento
                    meus_arquivos_estados[nome_arq] = {nova_v, tempo_atual};
                    
                } 
                // Se o arquivo JÁ EXISTE no mapa, verifica se o horário de modificação mudou
                else if (tempo_atual > meus_arquivos_estados[nome_arq].ultima_modificacao) {
                    std::cout << "\n[AUTO-SYNC] Mudança local detetada em: " << nome_arq << " (Ctrl+S funcionou!)\n";
                    
                    // Avisa o Tracker que o arquivo foi atualizado (sobe a versão)
                    int nova_v = tracker.call("atualizar_arquivo", minha_porta, nome_arq).as<int>();
                    int versao_atual = tracker.call("verificar_versao",nome_arq).as<int>();
                    meus_arquivos_estados[nome_arq].versao = versao_atual;
                    
                    // Atualiza o estado local para não disparar de novo no próximo segundo
                    meus_arquivos_estados[nome_arq].versao = nova_v;
                    meus_arquivos_estados[nome_arq].ultima_modificacao = tempo_atual;
                }
                else{
                    
                    int versao_rede = tracker.call("verificar_versao",nome_arq).as<int>();
                    if (versao_rede > meus_arquivos_estados[nome_arq].versao)
                    {
    
                        std::cout << "\n[AUTO-SYNC] Baixando versao atualizada (v" << versao_rede << ") de " << nome_arq << "...\n";
                        
                        fazer_download(nome_arq); 

                        // === A CORREÇÃO MÁGICA ENTRA AQUI ===
                        // Após o download terminar e o arquivo físico ter sido sobrescrito,
                        // lemos a NOVA data de modificação gerada pelo sistema operacional.
                        auto nova_data_fisica = std::filesystem::last_write_time(entrada.path());
                        
                        // Atualizamos nosso mapa para que o próximo loop ignore essa mudança de data
                        meus_arquivos_estados[nome_arq].versao = versao_rede;
                        meus_arquivos_estados[nome_arq].ultima_modificacao = nova_data_fisica;
                        
                        std::cout << "[AUTO-SYNC] Arquivo atualizado para a versão " << versao_rede << " com sucesso!\n";
                    }
                    
                    
                }
            }
        }
    }
}

// Função SERVIDORA deste peer: exposta para outros peers baixarem arquivos
// Função SERVIDORA deste peer: exposta para outros peers baixarem arquivos
std::vector<char> transferir_arquivo(std::string nome_arquivo) {
    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    std::cout << "[PEER SERVIDOR] Pedido de download recebido para: " << nome_arquivo << "\n";

    // Abre o arquivo em modo binário. 
    // O 'ate' (at end) já abre com o cursor no final para descobrirmos o tamanho do arquivo
    std::ifstream arquivo(caminho_completo, std::ios::binary | std::ios::ate);

    if (!arquivo.is_open()) {
        std::cout << "[ERRO] Arquivo solicitado nao foi encontrado no disco local.\n";
        return std::vector<char>(); // Retorna um vetor vazio se falhar
    }

    // Como abrimos com 'ate', tellg() nos dá o tamanho exato do arquivo em bytes
    std::streamsize tamanho = arquivo.tellg();
    
    // Volta o cursor de leitura para o começo do arquivo
    arquivo.seekg(0, std::ios::beg);

    // Cria um vetor com o tamanho exato necessário
    std::vector<char> buffer(tamanho);

    // Lê todos os bytes do arquivo físico e joga dentro do vetor
    if (arquivo.read(buffer.data(), tamanho)) {
        std::cout << "[PEER SERVIDOR] Enviando " << tamanho << " bytes reais pela rede.\n";
        return buffer;
    } else {
        std::cout << "[ERRO] Falha ao ler os bytes do arquivo.\n";
        return std::vector<char>();
    }
}

void cadastra_obj() {
    std::string arquivo;
    std::cout << "Digite o nome do arquivo a ser cadastrado na base de dados:\n";
    std::cin >> arquivo;

    std::string caminho_completo = meu_diretorio + "/" + arquivo;

    // NOVO: Verifica se o arquivo realmente existe fisicamente na pasta
    if (!std::filesystem::exists(caminho_completo)) {
        std::cout << "[ERRO] Arquivo '" << arquivo << "' nao encontrado na sua pasta local.\n";
        return; 
    }

    // Correção de int16_t para int
    int estado = tracker.call("registrar_peer", minha_porta, arquivo, upload).as<int>();

    if (estado == -1) {
        std::cout << "Arquivo ja registrado ou erro no tracker.\n";
    } else if (estado == 1) {
        std::cout << "Arquivo registrado na base de dados com sucesso!\n";
        meus_arquivos.push_back(arquivo);
    }
}


void aquisicao_obj() {
    
    std::string arquivo_requirido;
    std::cout << "Digite o nome do arquivo desejado:\n";
    std::cin >> arquivo_requirido;

    std::string caminho_completo = meu_diretorio + "/" + arquivo_requirido;

    // NOVO: Evita baixar um arquivo que você já tem na pasta
    if (std::filesystem::exists(caminho_completo)) {
        std::cout << "[PEER] Voce ja possui este arquivo salvo no disco!\n";
        return;
    }

    std::cout << "Fazendo a busca do arquivo " << arquivo_requirido << " no servidor...\n";
    auto portas = tracker.call("buscar_peers", minha_porta, arquivo_requirido).as<std::vector<int>>();

    if (!portas.empty()) {
        int porta_alvo = portas[0]; 
        std::cout << "[PEER] Arquivo encontrado na porta " << porta_alvo << ". Conectando...\n";
        
        try {
            rpc::client outro_peer("127.0.0.1", porta_alvo);
            auto dados_arquivo = outro_peer.call("transferir_arquivo", arquivo_requirido).as<std::vector<char>>();
            
            // === NOVO: SALVANDO O DOWNLOAD NO DISCO ===
            // std::ios::binary garante que o C++ não corrompa arquivos se não for TXT
            std::ofstream arquivo_saida(caminho_completo, std::ios::binary); 
            
            if (arquivo_saida.is_open()) {
                // Despeja o vetor de bytes recebido direto para dentro do arquivo
                arquivo_saida.write(dados_arquivo.data(), dados_arquivo.size());
                arquivo_saida.close();
                
                std::cout << "[PEER] Download concluido e salvo fisicamente! Recebidos " << dados_arquivo.size() << " bytes.\n";

                // AUTO-SEEDING
                tracker.call("registrar_peer", minha_porta, arquivo_requirido, download);
                meus_arquivos.push_back(arquivo_requirido);
                std::cout << "[PEER] Arquivo registrado. Voce agora e um seeder!\n";
            } else {
                std::cout << "[ERRO] Permissao negada para salvar o arquivo na pasta.\n";
            }

        } catch (const std::exception& e) {
            std::cout << "[ERRO] Falha ao conectar no Peer da porta " << porta_alvo << "\n";
            std::cout << "Motivo técnico: " << e.what() << "\n";
        }

    } else {
        std::cout << "[PEER] Ninguem na rede possui este arquivo no momento.\n";
    }
}

void criar_arquivo_txt() {
    std::string nome_arquivo;
    std::string conteudo;

    std::cout << "Digite o nome do novo arquivo (ex: meu_texto.txt): ";
    std::cin >> nome_arquivo;

    // Limpa o buffer do teclado antes de ler uma frase com espaços
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    std::cout << "Digite o conteudo do arquivo:\n";
    std::getline(std::cin, conteudo);

    // Cria o caminho completo (ex: ./pasta_Rai/meu_texto.txt)
    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    
    std::ofstream arquivo(caminho_completo); // Abre o arquivo para escrita
    
    if (arquivo.is_open()) {
        arquivo << conteudo; // Escreve o texto no arquivo físico
        arquivo.close();
        std::cout << "[SISTEMA] Arquivo '" << nome_arquivo << "' criado com sucesso no disco!\n";

        // Aproveita e já registra o arquivo recém-criado no Tracker
        int estado = tracker.call("registrar_peer", minha_porta, nome_arquivo, upload).as<int>();
        if (estado == 1) {
            meus_arquivos.push_back(nome_arquivo);
            std::cout << "[TRACKER] Arquivo disponibilizado na rede!\n";
        }
    } else {
        std::cout << "[ERRO] Falha ao criar o arquivo no disco.\n";
    }
}

// Cria um arquivo TXT com texto aleatório dentro da pasta do usuário
void criar_arquivo_aleatorio(std::string nome_arquivo) {
    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    
    // std::ofstream serve para escrever em arquivos
    std::ofstream arquivo(caminho_completo);
    if (arquivo.is_open()) {
        int numero_randomico = rand() % 10000; // Gera um número de 0 a 9999
        arquivo << "Este arquivo pertence a " << meu_usuario << "\n";
        arquivo << "Chave de seguranca (randomica): " << numero_randomico << "\n";
        arquivo.close();
        std::cout << "[SISTEMA] Arquivo '" << nome_arquivo << "' criado com sucesso.\n";
    } else {
        std::cout << "[ERRO] Nao foi possivel criar o arquivo no disco.\n";
    }
}

// Verifica se a pasta existe, cria se não existir, e registra o que achar
void inicializar_sistema_de_arquivos() {
    meu_diretorio = "./pasta_" + meu_usuario;

    // 1. Verifica se a pasta existe
    if (!fs::exists(meu_diretorio)) {
        std::cout << "[SISTEMA] Novo usuario detectado! Criando pasta: " << meu_diretorio << "\n";
        fs::create_directory(meu_diretorio);
        
        // Como é um usuário novo, vamos gerar um arquivo inicial para ele ter o que compartilhar
        criar_arquivo_aleatorio("boas_vindas.txt");
    } else {
        std::cout << "[SISTEMA] Bem-vindo de volta, " << meu_usuario << "!\n";
    }

    // 2. Varre a pasta inteira e registra tudo no Tracker
    std::cout << "[SISTEMA] Sincronizando arquivos locais com o Tracker...\n";
    
    // O directory_iterator passa por todos os arquivos dentro da pasta
    for (const auto& entrada : fs::directory_iterator(meu_diretorio)) {
        if (entrada.is_regular_file()) { // Garante que é um arquivo e não outra sub-pasta
            
            // Pega apenas o nome do arquivo (ex: "boas_vindas.txt") ignorando o caminho
            std::string nome_arq = entrada.path().filename().string(); 
            
            // Registra no tracker usando a lógica que já construímos
            tracker.call("registrar_peer", minha_porta, nome_arq, upload).as<int>();
            meus_arquivos.push_back(nome_arq);

            auto tempo_mod = std::filesystem::last_write_time(entrada.path());
            meus_arquivos_estados[nome_arq] = {1, tempo_mod}; 
            
            std::cout << " -> [" << nome_arq << "] registrado como Seeder.\n";
        }
    }
    std::cout << "[SISTEMA] Sincronizacao concluida. " << meus_arquivos.size() << " arquivos semeados.\n";
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "Uso: ./cliente <sua_porta> <nome_usuario>\n";
        std::cerr << "Exemplo: ./cliente 8081 Rai\n";
        return 1;
    }
    
    minha_porta = std::stoi(argv[1]);
    meu_usuario = argv[2]; // Salva o nome de usuário

    // Inicia a sincronização de pastas ANTES de abrir o menu!
    inicializar_sistema_de_arquivos();

    // --- 1. INICIA O SERVIDOR (Background) ---
    rpc::server srv(minha_porta);
    srv.bind("transferir_arquivo", &transferir_arquivo);
    srv.async_run(1); 
    std::cout << "[PEER] Escutando pedidos na porta " << minha_porta << "...\n";

    std::thread thread_sync(sincronizador_automatico);
    thread_sync.detach(); // Deixa-a correr livremente em background

    int op = 0; // CORREÇÃO: Variável inicializada

    while(op != -1){
        std::cout << "\n1: Cadastrar arquivo | 2: Baixar arquivo | 3: Criar aruivo | -1: Sair\nEscolha: ";
        std::cin >> op;

        switch (op) {
            case -1:
                std::cout << "Encerrando peer...\n";
                break;
            case 1:
                cadastra_obj();
                break;
            case 2:
                aquisicao_obj();
                break;
            case 3:
                criar_arquivo_txt();
            default:
                std::cout << "Operacao nao definida, tente novamente\n";
                break;
        }
    }
    
    return 0; // Removido o std::getline travado no final, o 'op == -1' já cuida de sair.
}