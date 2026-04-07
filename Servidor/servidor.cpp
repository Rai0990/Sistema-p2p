#include <iostream>
#include <vector>
#include <string>
#include <map>
#include "rpc/server.h"

// Banco de dados em memória: Nome do Arquivo -> Lista de Portas que o possuem
std::map<std::string, std::vector<int>> tabela_arquivos;

// Função chamada pelos Peers quando entram na rede
std::int16_t registrar_peer(int porta_peer, std::string arquivo) {
    
    if (tabela_arquivos.find(arquivo) != tabela_arquivos.end()) {
        return -1;
    }
    else{
        
        tabela_arquivos[arquivo].push_back(porta_peer);
        std::cout << "[TRACKER] Peer na porta " << porta_peer << " registrou " << arquivo << " arquivos.\n";
        
        return 1;
    }
}

// Função chamada pelos Peers para descobrir com quem baixar
std::vector<int> buscar_peers(int porta_peer, std::string nome_arquivo) {
    std::cout << "cliente porta " << porta_peer << " pede arquivo " << nome_arquivo << " na base de dados\n";
    if (tabela_arquivos.find(nome_arquivo) != tabela_arquivos.end()) {
        std::cout << "cliente porta " << porta_peer << " sucesso";
        return tabela_arquivos[nome_arquivo];
    }
    std::cout << "cliente porta " << porta_peer << " falha";
    return {}; // Retorna lista vazia se ninguém tiver o arquivo
}

int main() {
    rpc::server srv(8000); // Controlador sempre roda na porta conhecida: 8000

    // Mapeia as funções C++ para chamadas RPC
    srv.bind("registrar_peer", &registrar_peer);
    srv.bind("buscar_peers", &buscar_peers);

    std::cout << "[TRACKER] Iniciado na porta 8000. Aguardando peers...\n";
    srv.run(); // Função síncrona, bloqueia e fica escutando requisições

    return 0;
}