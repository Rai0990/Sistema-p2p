#include <iostream>
#include <vector>
#include <string>
#include <map>
#include "rpc/server.h"
#include <algorithm>
#include <nlohmann/json.hpp>
#include <fstream> // Para ler e escrever o arquivo .json#include <nlohmann/json.hpp>

using json = nlohmann::json;

#define download 1
#define upload 2

struct MetadadosArquivo {
    std::string criador;
    int tamanho_bytes;
    int versao;
    std::vector<int> seeders;
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MetadadosArquivo, criador, tamanho_bytes, versao, seeders)

// Banco de dados em memória: Nome do Arquivo -> Lista de Portas que o possuem
std::map<std::string, MetadadosArquivo> tabela_arquivos;

// Salva todo o mapa atual num arquivo físico chamado "tracker_state.json"
void salvar_estado() {
    std::ofstream arquivo("tracker_state.json");
    if (arquivo.is_open()) {
        json j = tabela_arquivos; // Converte o mapa inteiro para JSON
        arquivo << j.dump(4);     // Grava no arquivo com indentação de 4 espaços (bonito para ler)
        arquivo.close();
    }
}

// Lê o arquivo JSON quando o Tracker é reiniciado e devolve para a memória
void carregar_estado() {
    std::ifstream arquivo("tracker_state.json");
    if (arquivo.is_open()) {
        json j;
        arquivo >> j; // Lê o texto do arquivo para o objeto json
        tabela_arquivos = j.get<std::map<std::string, MetadadosArquivo>>(); // Converte de volta para C++
        arquivo.close();
        std::cout << "[TRACKER] Estado anterior carregado! " << tabela_arquivos.size() << " arquivos conhecidos.\n";
    } else {
        std::cout << "[TRACKER] Nenhum estado anterior encontrado. Iniciando base limpa.\n";
    }
}

int atualizar_arquivo(int porta_peer, std::string arquivo) {
    // Verifica se o arquivo realmente existe na rede
    if (tabela_arquivos.find(arquivo) != tabela_arquivos.end()) {
        auto& meta = tabela_arquivos[arquivo];
        
        meta.versao += 1; // Sobe a versão do arquivo
        
        // CRÍTICO: Limpa a lista de seeders antigos, pois os arquivos deles estão desatualizados!
        meta.seeders.clear(); 
        
        // Adiciona apenas você como seeder da nova versão
        meta.seeders.push_back(porta_peer);
        
        std::cout << "[TRACKER] Peer " << porta_peer << " ATUALIZOU o arquivo '" << arquivo << "' para a versao " << meta.versao << ".\n";
        
        salvar_estado(); // Grava no JSON
        return meta.versao; // Retorna a nova versão para o cliente
    }
    
    return -1; // Erro: Tentou atualizar um arquivo que não existe no Tracker
}

int verificar_versao(std::string arquivo) {
    if (tabela_arquivos.find(arquivo) != tabela_arquivos.end()) {
        return tabela_arquivos[arquivo].versao;
    }
    return 0; // Retorna 0 se o arquivo não existe na rede
}

// Função chamada pelos Peers quando entram na rede
int registrar_peer(int porta_peer, std::string arquivo, int op) {
    
    // Se o arquivo NÃO existe na base, criamos os metadados do zero
    if (tabela_arquivos.find(arquivo) == tabela_arquivos.end()) {
        if (op == upload) {
            MetadadosArquivo meta;
            meta.criador = "Porta_" + std::to_string(porta_peer); // Simplificação inicial
            meta.tamanho_bytes = 0; // (Poderemos passar o tamanho real futuramente via RPC)
            meta.versao = 1;        // Começa na versão 1
            meta.seeders.push_back(porta_peer);
            
            tabela_arquivos[arquivo] = meta;
            std::cout << "[TRACKER] Novo arquivo '" << arquivo << "' registrado v" << meta.versao << ".\n";
        } else {
            return -1; // Tentou fazer download/registrar algo que não existe
        }
    } 
    // Se o arquivo JÁ EXISTE, apenas adicionamos o peer na lista de seeders (se ele já não estiver lá)
    else {
        auto& seeders = tabela_arquivos[arquivo].seeders;
        if (std::find(seeders.begin(), seeders.end(), porta_peer) == seeders.end()) {
            seeders.push_back(porta_peer);
            std::cout << "[TRACKER] Peer " << porta_peer << " virou seeder de '" << arquivo << "'.\n";
        }
    }

    // SALVA NO DISCO SEMPRE QUE A BASE MUDAR!
    salvar_estado();
    return 1;
}

// Função chamada pelos Peers para descobrir com quem baixar
std::vector<int> buscar_peers(int porta_peer, std::string nome_arquivo) {
    std::cout << "cliente porta " << porta_peer << " pede arquivo " << nome_arquivo << " na base de dados\n";
    if (tabela_arquivos.find(nome_arquivo) != tabela_arquivos.end()) {
        std::cout << "cliente porta " << porta_peer << " sucesso\n";
        return tabela_arquivos[nome_arquivo].seeders;
    }
    std::cout << "cliente porta " << porta_peer << " falha\n";
    return {}; // Retorna lista vazia se ninguém tiver o arquivo
}

int main() {
    rpc::server srv(8000); // Controlador sempre roda na porta conhecida: 8000
    carregar_estado();

    // Mapeia as funções C++ para chamadas RPC
    srv.bind("registrar_peer", &registrar_peer);
    srv.bind("buscar_peers", &buscar_peers);
    srv.bind("atualizar_arquivo", &atualizar_arquivo); // <--- Faltava este!
    srv.bind("verificar_versao", &verificar_versao);

    std::cout << "[TRACKER] Iniciado na porta 8000. Aguardando peers...\n";
    srv.run(); // Função síncrona, bloqueia e fica escutando requisições

    return 0;
}