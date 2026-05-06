#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <fstream>
#include <thread> 
#include <chrono> 
#include <shared_mutex>  
#include <mutex>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

#include "rpc/server.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

#define download 1
#define upload 2
#define privado 3
#define somente_leitura 4
#define escrita 5

struct MetadadosArquivo {
    std::string criador;
    int tamanho_bytes;
    int versao;
    int permisao;
    std::vector<std::string> usuarios_seeders; 
};

struct Usuario {
    std::string nome;
    std::string ip;
    int porta;
    int status_vivo; // 1 = Vivo, 0 = Ausente
};

NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(MetadadosArquivo, criador, tamanho_bytes, versao, permisao, usuarios_seeders)

std::map<std::string, MetadadosArquivo> tabela_arquivos;
std::map<std::string, Usuario> tabela_usuarios_conectados;
std::string meu_ip_atual;
// Cadeado para proteger a tabela_usuarios_conectados de colisões de Threads
std::shared_mutex mtx_usuarios;

// Função chamada pelo Cliente logo ao abrir o terminal
void registrar_usuario_rede(std::string nome, std::string ip, int porta) {
    std::unique_lock<std::shared_mutex> lock_escrita(mtx_usuarios); // Tranca antes de mexer
    
    // Cadastra já com status_vivo = 1
    tabela_usuarios_conectados[nome] = {nome, ip, porta, 1};
    std::cout << "[TRACKER] Usuario " << nome << " logado em " << ip << ":" << porta << "\n";
}

// mesmo código de obtenção de IP do cliente
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

// Função para o Tracker atualizar o IP caso o notebook do cliente mude de rede
int atualizar_ip_usuario(std::string nome, std::string novo_ip) {
    std::lock_guard<std::shared_mutex> lock_escrita(mtx_usuarios);

    if (tabela_usuarios_conectados.count(nome)) {
        tabela_usuarios_conectados[nome].ip = novo_ip;
        std::cout << "[TRACKER] Usuario " << nome << " mudou de rede. Novo IP: " << novo_ip << "\n";
        return 1;
    }

    return 0;
}

void salvar_estado() {
    std::ofstream arquivo("tracker_state.json");
    if (arquivo.is_open()) {
        json j = tabela_arquivos; 
        arquivo << j.dump(4);     
        arquivo.close();
    }
}

void carregar_estado() {
    std::ifstream arquivo("tracker_state.json");
    if (arquivo.is_open()) {
        json j;
        arquivo >> j; 
        tabela_arquivos = j.get<std::map<std::string, MetadadosArquivo>>(); 
        arquivo.close();
        std::cout << "[TRACKER] Estado anterior carregado! " << tabela_arquivos.size() << " arquivos conhecidos.\n";
    } else {
        std::cout << "[TRACKER] Nenhum estado anterior encontrado. Iniciando base limpa.\n";
    }
}

int atualizar_arquivo(std::string nome_usuario, std::string arquivo,int tamanho) {
    if (tabela_arquivos.find(arquivo) != tabela_arquivos.end()) {
        auto& meta = tabela_arquivos[arquivo];
        
        // Se NÃO é o dono E a permissão NÃO é de escrita pública -> Bloqueia!
        if (meta.criador != nome_usuario && meta.permisao != escrita) {
            std::cout << "[TRACKER] BLOQUEADO: " << nome_usuario << " tentou modificar '" << arquivo << "' (Dono: " << meta.criador << ")\n";
            return -2; 
        }

        meta.versao += 1; 
        meta.usuarios_seeders.clear(); 
        meta.usuarios_seeders.push_back(nome_usuario);
        meta.tamanho_bytes = tamanho;
        
        std::cout << "[TRACKER] " << nome_usuario << " ATUALIZOU '" << arquivo << "' para v" << meta.versao << ".\n";
        salvar_estado(); 
        return meta.versao; 
    }
    return -1; 
}

int verificar_versao(std::string nome_usuario, std::string arquivo) {
    
    if (tabela_arquivos.find(arquivo) != tabela_arquivos.end()) {
        return tabela_arquivos[arquivo].versao;
    }
    return 0; 
}

int registrar_peer(std::string nome_usuario, std::string arquivo,int permisao, int op,int tamanho) {
    if (tabela_arquivos.find(arquivo) == tabela_arquivos.end()) {
        if (op == upload) {
            MetadadosArquivo meta;
            meta.criador = nome_usuario; // O usuário vira o dono soberano
            meta.tamanho_bytes = 0; 
            meta.versao = 1;
            meta.tamanho_bytes = tamanho;  
            meta.permisao = permisao;      
            meta.usuarios_seeders.push_back(nome_usuario);
            
            tabela_arquivos[arquivo] = meta;
            std::cout << "[TRACKER] Novo arquivo '" << arquivo << "' registrado por " << nome_usuario << ".\n";
        } else {
            return -1;
        }
    } 
    else if(tabela_arquivos[arquivo].criador == nome_usuario){
        tabela_arquivos[arquivo].permisao = permisao;
    }
    else{
        auto& seeders = tabela_arquivos[arquivo].usuarios_seeders;
        if (std::find(seeders.begin(), seeders.end(), nome_usuario) == seeders.end()) {
            seeders.push_back(nome_usuario);
            std::cout << "[TRACKER] " << nome_usuario << " virou seeder de '" << arquivo << "'.\n";
        }
    }

    salvar_estado();
    return 1;
}

std::vector<std::pair<std::string, int>> buscar_peers(std::string nome_usuario, std::string nome_arquivo) {
    std::vector<std::pair<std::string, int>> enderecos_disponiveis;
    
    std::cout << "O usuario " << nome_usuario << " pede enderecos para o arquivo " << nome_arquivo << "...\n";
    
    if (tabela_arquivos.find(nome_arquivo) != tabela_arquivos.end()) {
        if(tabela_arquivos[nome_arquivo].permisao != privado || tabela_arquivos[nome_arquivo].criador == nome_usuario){    
            // Colocamos o cadeado AQUI, antes de varrer e acessar a tabela de usuários!
            std::shared_lock<std::shared_mutex> lock_leitura(mtx_usuarios); 
            
            for (const auto& nome_seeder : tabela_arquivos[nome_arquivo].usuarios_seeders) {
                
                // O .count() e o acesso [] agora estão 100% protegidos contra o Ceifador
                if (nome_seeder != nome_usuario && tabela_usuarios_conectados.count(nome_seeder)) {
                    Usuario u = tabela_usuarios_conectados[nome_seeder];
                    enderecos_disponiveis.push_back({u.ip, u.porta});
                }
            }
        }
        else{
            std::cout << "[TRACKER] " << nome_usuario << " nao tem permissao de download do arquivo " << nome_arquivo << ".\n";
        }
    }
    
    return enderecos_disponiveis;
}

void heartbeat(std::string nome, std::string ip, int porta){

    mtx_usuarios.lock();
    if (tabela_usuarios_conectados.find(nome) != tabela_usuarios_conectados.end())
    {
        tabela_usuarios_conectados[nome].status_vivo = 1;
        mtx_usuarios.unlock();
    }
    else{
        mtx_usuarios.unlock();
        registrar_usuario_rede(nome,ip,porta);
    }
    return;
}

void limpador_de_peers_inativos() {
    while (true) {
        // Roda o varredor a cada 15 segundos
        // (Recomendo deixar um tempo maior que o sleep do Cliente para evitar falsos positivos)
        std::this_thread::sleep_for(std::chrono::seconds(15));

        std::lock_guard<std::shared_mutex> lock_escrita(mtx_usuarios); // Tranca o mapa para a varredura
        
        // Usamos um iterador clássico porque vamos apagar itens do mapa enquanto andamos por ele
        for (auto it = tabela_usuarios_conectados.begin(); it != tabela_usuarios_conectados.end(); ) {
            
            if (it->second.status_vivo == 0) {
                // Se continuou 0 desde a última varredura, o usuário morreu/desconectou
                std::cout << "[HEARTBEAT] Usuario '" << it->first << "' n\xc3\xa3o respondeu. Removendo da rede...\n";
                
                it = tabela_usuarios_conectados.erase(it); // Remove e avança o ponteiro
            } 
            else {
                // Se estava 1, ele está vivo! Mas setamos para 0 para testá-lo no próximo ciclo
                it->second.status_vivo = 0;
                ++it; // Avança o ponteiro
            }
        }
    }
}

int obter_tamanho_arquivo(std::string arquivo) {
    if (tabela_arquivos.find(arquivo) != tabela_arquivos.end()) {
        return tabela_arquivos[arquivo].tamanho_bytes;
    }
    return 0;
}

int main() {
    rpc::server srv(8000); 
    carregar_estado();

    meu_ip_atual = obter_meu_ip(); // Descobre o IP real nativamente

    std::cout << "[SISTEMA] Inicializando com IP Local: " << meu_ip_atual << "\n";

    // Binds atualizados
    srv.bind("registrar_usuario_rede", &registrar_usuario_rede);
    srv.bind("registrar_peer", &registrar_peer);
    srv.bind("buscar_peers", &buscar_peers);
    srv.bind("atualizar_arquivo", &atualizar_arquivo); 
    srv.bind("verificar_versao", &verificar_versao);
    srv.bind("atualizar_ip_usuario",&atualizar_ip_usuario);
    srv.bind("obter_tamanho_arquivo",&obter_tamanho_arquivo);
    srv.bind("heartbeat",&heartbeat);
    std::cout << "[TRACKER] Iniciando sistema de Heartbeat...\n";
    //lançamento da thread paralela
    std::thread thread_heartbeat(limpador_de_peers_inativos);
    thread_heartbeat.detach();
    std::cout << "[TRACKER] Iniciado na porta 8000. Aguardando peers...\n";
    srv.run(); 

    return 0;
}