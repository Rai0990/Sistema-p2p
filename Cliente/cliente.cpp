#include <iostream>
#include <vector>
#include <string>
#include "rpc/server.h"
#include "rpc/client.h"
#include <exception>

#define download 1
#define upload 2

// --- 2. COMUNICAÇÃO COM O TRACKER (Cliente) ---
rpc::client tracker("127.0.0.1", 8000);
int minha_porta;
std::vector<std::string> meus_arquivos;

// Função SERVIDORA deste peer: exposta para outros peers baixarem arquivos
std::vector<char> transferir_arquivo(std::string nome_arquivo) {
    // Aqui entrará a lógica real de leitura do disco futuramente.
    // Simulando a leitura de um arquivo em bytes:
    std::string conteudo = "Conteudo binario do arquivo " + nome_arquivo;
    std::cout << "[PEER] Enviando arquivo: " << nome_arquivo << "\n";
    return std::vector<char>(conteudo.begin(), conteudo.end());
}

void cadastra_obj(){
    std::string arquivo;

    std::cout << "digite o nome do arquivo a ser cadastrado na base de dados\n";
    std::cin >> arquivo;

    int estado = tracker.call("registrar_peer",minha_porta,arquivo,upload).as<std::int16_t>();

    if (estado == -1)
    {
        std::cout << "Arquivo nao registrado, renomeie o arquivo e tente novamente\n";
    }
    else if(estado == 1){
        std::cout << "Arquivo registrado na base de dados com sucesso\n";
        meus_arquivos.push_back(arquivo);
    }
    
    return;
}

void aquisicao_obj() {
    std::string arquivo_requirido;
    std::cout << "Digite o nome do arquivo desejado\n";
    std::cin >> arquivo_requirido;

    std::cout << "Fazendo a busca do arquivo "<< arquivo_requirido << " no servidor...\n";

    auto portas = tracker.call("buscar_peers", minha_porta, arquivo_requirido).as<std::vector<int>>();

    if (!portas.empty()) {
        int porta_alvo = portas[0]; // Pega o primeiro peer da lista (poderia ser aleatório no futuro)
        std::cout << "[PEER] Arquivo encontrado na porta " << porta_alvo << ". Conectando...\n";
        
        try {
            // Conecta DIRETAMENTE no outro peer, ignorando o tracker
            rpc::client outro_peer("127.0.0.1", porta_alvo);
            auto dados_arquivo = outro_peer.call("transferir_arquivo", arquivo_requirido).as<std::vector<char>>();
            
            std::cout << "[PEER] Download concluido! Recebidos " << dados_arquivo.size() << " bytes.\n";

            // AUTO-SEEDING: Avisa o tracker que agora você também é fonte deste arquivo!
            tracker.call("registrar_peer", minha_porta, arquivo_requirido,download);
            meus_arquivos.push_back(arquivo_requirido);
            std::cout << "[PEER] Arquivo registrado no tracker localmente. Voce agora e um seeder!\n";

        } catch (const std::exception& e) {
            std::cout << "[ERRO] Falha ao conectar no Peer da porta " << porta_alvo << "\n";
            std::cout << "Motivo técnico: " << e.what() << "\n";
        }

    } else {
        std::cout << "[PEER] Ninguem na rede possui este arquivo no momento.\n";
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cerr << "Uso: ./peer <sua_porta>\n";
        return 1;
    }
    minha_porta = std::stoi(argv[1]);

    // --- 1. INICIA O SERVIDOR (Background) ---
    rpc::server srv(minha_porta);
    srv.bind("transferir_arquivo", &transferir_arquivo);
    srv.async_run(1); 
    std::cout << "[PEER] Escutando pedidos na porta " << minha_porta << "...\n";

    int op = 0; // CORREÇÃO: Variável inicializada

    while(op != -1){
        std::cout << "\n1: Cadastrar arquivo | 2: Baixar arquivo | -1: Sair\nEscolha: ";
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
            default:
                std::cout << "Operacao nao definida, tente novamente\n";
                break;
        }
    }
    
    return 0; // Removido o std::getline travado no final, o 'op == -1' já cuida de sair.
}