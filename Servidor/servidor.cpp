#include <iostream>
#include <vector>
#include <string>
#include <map>
#include "rpc/server.h"
#include <algorithm>

#define download 1
#define upload 2

// Banco de dados em memória: Nome do Arquivo -> Lista de Portas que o possuem
std::map<std::string, std::vector<int>> tabela_arquivos;

// Função chamada pelos Peers quando entram na rede
std::int16_t registrar_peer(int porta_peer, std::string arquivo, int op) {
    
    if(upload == op){
        if (tabela_arquivos.find(arquivo) != tabela_arquivos.end()) {
        return -1;
        }
        else{
            
            tabela_arquivos[arquivo].push_back(porta_peer);
            std::cout << "[TRACKER] Peer na porta " << porta_peer << " registrou " << arquivo << " arquivos.\n";
            
            return 1;
        }

    }
    else if(download == op){
        // Acessa a lista de portas para este arquivo (se não existir, o C++ cria uma lista vazia)
        auto& lista_portas = tabela_arquivos[arquivo];

        // Verifica se ESTA porta já está na lista deste arquivo para evitar duplicatas
        if (std::find(lista_portas.begin(), lista_portas.end(), porta_peer) != lista_portas.end()) {
            std::cout << "[TRACKER] O Peer da porta " << porta_peer << " ja havia registrado o arquivo " << arquivo << ".\n";
            return 0; // Retorna 0 (ou outro código de sua escolha) indicando que já estava cadastrado
        }

        // Se não encontrou a porta na lista, adiciona! 
        // (Funciona tanto para o primeiro peer quanto para o décimo peer com o mesmo arquivo)
        lista_portas.push_back(porta_peer);
        
        std::cout << "[TRACKER] Peer na porta " << porta_peer << " registrou o arquivo: " << arquivo << "\n";
        
        return 1; // Sucesso
    }
    return -1;
}

// Função chamada pelos Peers para descobrir com quem baixar
std::vector<int> buscar_peers(int porta_peer, std::string nome_arquivo) {
    std::cout << "cliente porta " << porta_peer << " pede arquivo " << nome_arquivo << " na base de dados\n";
    if (tabela_arquivos.find(nome_arquivo) != tabela_arquivos.end()) {
        std::cout << "cliente porta " << porta_peer << " sucesso\n";
        return tabela_arquivos[nome_arquivo];
    }
    std::cout << "cliente porta " << porta_peer << " falha\n";
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