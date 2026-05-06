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
#include <algorithm>
#include <atomic>
#include <nlohmann/json.hpp>

// funções para uso de portas udp
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>

// função para adição das chamadas rpc
#include "rpc/server.h"
#include "rpc/client.h"

using json = nlohmann::json;
namespace fs = std::filesystem;
using Endereco = std::pair<std::string, int>; // estrutura para guardar pares de ip e porta

// definições de constantes utilizadas nos códigos
#define download 1
#define upload 2
#define privado 3
#define somente_leitura 4
#define escrita 5

#define PAGINA 64000 // 256 BYTES
#define PORTA_SERVIDOR 8000

// estrutura de dados da tabela local
struct EstadoLocal {
    int versao;
    std::filesystem::file_time_type ultima_modificacao;
    int permissao;
    int tamanho_bytes; 
};

int minha_porta;
std::string meu_usuario;
std::string meu_diretorio;
std::string meu_ip_atual;
std::mutex acesso_tabela; 
std::mutex adiquirir_pagina;
std::map<std::string, EstadoLocal> meus_arquivos_estados;
rpc::client tracker("192.168.1.20", 8000); // Endereço e porta do servidor, teria como definir na main, mas teria que ser por ponteiro e toda a estrutura lógica já foi montada sem ser por ponteiro

// Variáveis de bloqueio de verificação de arquivos que estão fazendo download
std::mutex verifica_download;
bool esta_em_download = false;

// Salvamento dos metadados no formato json
void salvar_estado_local() {
    std::lock_guard<std::mutex> lock(acesso_tabela); 
    json j;
    
    for (const auto& [nome, estado] : meus_arquivos_estados) {
        j[nome] = {
            {"versao", estado.versao},
            {"permissao", estado.permissao},
            {"tamanho_bytes", estado.tamanho_bytes},
            {"ultima_modificacao", estado.ultima_modificacao.time_since_epoch().count()}
        };
    }
    
    std::string arquivo_estado = "estado_cliente_" + meu_usuario + ".json";
    std::ofstream file(arquivo_estado);
    if (file.is_open()) {
        file << j.dump(4);
    }
}

void carregar_estado_local() {
    std::string arquivo_estado = "estado_cliente_" + meu_usuario + ".json";
    std::ifstream file(arquivo_estado);
    
    if (file.is_open()) {
        json j;
        file >> j;
        
        std::lock_guard<std::mutex> lock(acesso_tabela);
        for (auto& el : j.items()) {
            EstadoLocal est;
            est.versao = el.value()["versao"];
            est.permissao = el.value()["permissao"];
            
            if (el.value().contains("tamanho_bytes")) {
                est.tamanho_bytes = el.value()["tamanho_bytes"];
            } else {
                est.tamanho_bytes = 0;
            }
            
            long long ticks = el.value()["ultima_modificacao"];
            est.ultima_modificacao = std::filesystem::file_time_type(std::filesystem::file_time_type::duration(ticks));
            
            meus_arquivos_estados[el.key()] = est;
        }
        std::cout << "[SISTEMA] Estado local recuperado. " << meus_arquivos_estados.size() << " arquivos na memoria.\n";
    }
}

// Função teste de performance de peers, com ele é possível ver se o peer consegue fazer a entrega da página no instante i
void servidor_udp_ping(int porta_rpc_base) {
    int porta_udp = porta_rpc_base + 10000;
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) return;

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(porta_udp);
    
    bind(sockfd, (const struct sockaddr *)&server_addr, sizeof(server_addr));

    char buffer[10];
    sockaddr_in client_addr;
    socklen_t len = sizeof(client_addr);
    
    while(true) {
        int n = recvfrom(sockfd, (char *)buffer, 10, MSG_WAITALL, (struct sockaddr *) &client_addr, &len);
        if (n > 0) {
            buffer[n] = '\0';
            if (strcmp(buffer, "PING") == 0) {
                sendto(sockfd, "PONG", 4, MSG_CONFIRM, (const struct sockaddr *) &client_addr, len);
            }
        }
    }
}

// declarado como constante pois os ip e portas retornados pelo servidor não vão ser alterados
std::vector<Endereco> filtrar_peers_por_latencia(const std::vector<Endereco>& peers, int timeout_ms) {
    std::vector<Endereco> peers_aprovados;
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) return peers_aprovados;

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    for (const auto& peer : peers) {
        std::string ip = peer.first;
        int porta_rpc = peer.second;
        int porta_udp = porta_rpc + 10000;

        sockaddr_in servaddr{};
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(porta_udp);
        inet_pton(AF_INET, ip.c_str(), &servaddr.sin_addr);

        auto inicio = std::chrono::high_resolution_clock::now();
        sendto(sockfd, "PING", 4, MSG_CONFIRM, (const struct sockaddr *) &servaddr, sizeof(servaddr));
        
        char buffer[10];
        socklen_t len = sizeof(servaddr);
        int n = recvfrom(sockfd, (char *)buffer, 10, MSG_WAITALL, (struct sockaddr *) &servaddr, &len);
        auto fim = std::chrono::high_resolution_clock::now();

        if (n > 0) {
            buffer[n] = '\0';
            if (strcmp(buffer, "PONG") == 0) {
                auto latencia = std::chrono::duration_cast<std::chrono::milliseconds>(fim - inicio).count();
                std::cout << "[UDP-PING] Peer " << ip << ":" << porta_rpc << " respondeu em " << latencia << "ms.\n";
                if (latencia <= timeout_ms) {
                    peers_aprovados.push_back(peer);
                }
            }
        } else {
            std::cout << "[UDP-PING] Timeout/Falha no Peer " << ip << ":" << porta_rpc << " (>" << timeout_ms << "ms).\n";
        }
    }
    close(sockfd);
    return peers_aprovados;
}

// função de descoberta do ip local da máquina
std::string obter_meu_ip() {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) return "127.0.0.1";

    struct sockaddr_in serv;
    memset(&serv, 0, sizeof(serv));
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr("8.8.8.8"); 
    serv.sin_port = htons(53);

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
// 2. LÓGICA DE DOWNLOAD (Com Fila Dinâmica e Cronômetro)
// ========================================================
int fazer_download(std::string nome_arq, rpc::client& tracker_ativo) {
    verifica_download.lock();
    esta_em_download = true;
    verifica_download.unlock();

    std::string caminho_completo = meu_diretorio + "/" + nome_arq;
    
    auto enderecos_brutos = tracker_ativo.call("buscar_peers", meu_usuario, nome_arq).as<std::vector<Endereco>>();
    if (enderecos_brutos.empty()) {
        std::cout << "[PEER] O dono do arquivo esta offline.\n";
        verifica_download.lock(); esta_em_download = false; verifica_download.unlock();
        return 0;
    }

    auto enderecos = enderecos_brutos; 
    if (enderecos.empty()) {
        verifica_download.lock(); esta_em_download = false; verifica_download.unlock();
        return 0;
    }

    int tamanho_total = tracker_ativo.call("obter_tamanho_arquivo", nome_arq).as<int>();
    if (tamanho_total <= 0) {
        std::cout << "[ERRO] Tamanho do arquivo indisponivel no servidor.\n";
        verifica_download.lock(); esta_em_download = false; verifica_download.unlock();
        return 0;
    }

    int num_paginas = (tamanho_total + PAGINA - 1) / PAGINA; 
    
    std::cout << "[PEER] Iniciando download de '" << nome_arq << "' (" << tamanho_total << " bytes em " << num_paginas << " paginas)...\n";

    std::map<std::string, int> estatisticas_download;
    std::mutex trava_estatisticas; 
    
    std::cout << "\n[TELEMETRIA] Peers disponiveis para esta transferencia:\n";
    for (const auto& peer : enderecos) {
        std::string chave_peer = peer.first + ":" + std::to_string(peer.second);
        estatisticas_download[chave_peer] = 0;
        std::cout << " -> " << chave_peer << "\n";
    }
    std::cout << "\n";

    {
        std::ofstream arquivo_criar(caminho_completo, std::ios::binary);
        arquivo_criar.close(); 
        std::filesystem::resize_file(caminho_completo, tamanho_total); 
    }

    std::mutex trava_escrita_arquivo; 
    bool falha_critica = false;
    
    std::vector<int> paginas_pendentes;
    for (int i = 0; i < num_paginas; i++) {
        paginas_pendentes.push_back(i);
    }

    int rodadas_sem_sucesso = 0;
    int peer_offset = 0; 

    // ========================================================
    // INÍCIO DO CRONÔMETRO
    // ========================================================
    auto tempo_inicio = std::chrono::high_resolution_clock::now();

    while (!paginas_pendentes.empty()) {
        std::vector<std::thread> lote_threads;
        std::mutex trava_falhas;
        std::vector<int> paginas_com_falha;

        int tamanho_lote = std::min((int)paginas_pendentes.size(), (int)enderecos.size());

        for (int i = 0; i < tamanho_lote; i++) {
            int pagina_atual = paginas_pendentes[i];
            Endereco peer_alvo = enderecos[(peer_offset + i) % enderecos.size()];

            lote_threads.push_back(std::thread([&, pagina_atual, peer_alvo]() {
                bool sucesso = false;
                std::string chave_peer = peer_alvo.first + ":" + std::to_string(peer_alvo.second);

                try {
                    rpc::client outro_peer(peer_alvo.first, peer_alvo.second);
                    auto dados_pagina = outro_peer.call("transferir_pagina", nome_arq, pagina_atual).as<std::vector<char>>();

                    if (!dados_pagina.empty()) {
                        std::lock_guard<std::mutex> lock(trava_escrita_arquivo);
                        std::fstream arquivo_saida(caminho_completo, std::ios::binary | std::ios::in | std::ios::out);
                        arquivo_saida.seekp(pagina_atual * PAGINA, std::ios::beg);
                        arquivo_saida.write(dados_pagina.data(), dados_pagina.size());
                        sucesso = true; 
                    }
                } catch (...) {
                    // Falhou, 'sucesso' continua false
                }

                if (sucesso) {
                    std::lock_guard<std::mutex> lock_stats(trava_estatisticas);
                    estatisticas_download[chave_peer]++;
                } else {
                    std::lock_guard<std::mutex> lock_falha(trava_falhas);
                    paginas_com_falha.push_back(pagina_atual);
                }
            }));
        }

        for (auto& t : lote_threads) {
            if (t.joinable()) t.join();
        }

        peer_offset = (peer_offset + tamanho_lote) % enderecos.size();
        paginas_pendentes.erase(paginas_pendentes.begin(), paginas_pendentes.begin() + tamanho_lote);

        for (int pag_quebrada : paginas_com_falha) {
            paginas_pendentes.push_back(pag_quebrada);
        }

        if (paginas_com_falha.size() == tamanho_lote) {
            rodadas_sem_sucesso++;
            if (rodadas_sem_sucesso >= 3) { 
                std::cout << "[ERRO] Multiplos peers falharam. Limite de tentativas excedido.\n";
                falha_critica = true;
                break;
            }
        } else {
            rodadas_sem_sucesso = 0; 
        }
    }

    // ========================================================
    // FIM DO CRONÔMETRO E CÁLCULO
    // ========================================================
    auto tempo_fim = std::chrono::high_resolution_clock::now();
    long long duracao_ms = std::chrono::duration_cast<std::chrono::milliseconds>(tempo_fim - tempo_inicio).count();
    
    // Evita divisão por zero se o download for rápido demais (< 1ms)
    double segundos_decorridos = std::max(duracao_ms / 1000.0, 0.001); 
    
    // Calcula Megabytes e Velocidade
    double megabytes_total = tamanho_total / (1024.0 * 1024.0);
    double velocidade_mbps = megabytes_total / segundos_decorridos;

    if (falha_critica) {
        std::cout << "\n[FALHA FATAL] O download nao pode ser concluido. Removendo arquivo corrompido do disco...\n";
        std::filesystem::remove(caminho_completo); 

        verifica_download.lock();
        esta_em_download = false;
        verifica_download.unlock();

        return 0; 
    }

    std::cout << "\n=========================================\n";
    std::cout << "          RELATORIO DE DOWNLOAD          \n";
    std::cout << "=========================================\n";
    std::cout << "Tempo total   : " << duracao_ms << " ms (" << segundos_decorridos << " segundos)\n";
    std::cout << "Velocidade    : ";
    if (velocidade_mbps >= 1.0) {
        printf("%.2f MB/s\n", velocidade_mbps);
    } else {
        printf("%.2f KB/s\n", velocidade_mbps * 1024.0); // Converte para KB/s se for muito pequeno
    }
    std::cout << "-----------------------------------------\n";
    
    for (const auto& [peer, paginas_baixadas] : estatisticas_download) {
        if (paginas_baixadas > 0) { 
            double porcentagem = ((double)paginas_baixadas / num_paginas) * 100.0;
            std::cout << "[+] " << peer << " forneceu " << paginas_baixadas << " paginas (";
            printf("%.1f%%", porcentagem); 
            std::cout << " do arquivo)\n";
        }
    }
    std::cout << "=========================================\n\n";

    int tamanho_baixado = (int)std::filesystem::file_size(caminho_completo);
    
    // Usa a permissão real (Correção que fizemos anteriormente)
    int permissao_real = tracker_ativo.call("obter_permissao_arquivo", nome_arq).as<int>();
    tracker_ativo.call("registrar_peer", meu_usuario, nome_arq, permissao_real, (int)download, (int)tamanho_baixado);

    acesso_tabela.lock();
    int versao = tracker_ativo.call("verificar_versao", meu_usuario, nome_arq).as<int>();
    meus_arquivos_estados[nome_arq] = {versao, std::filesystem::last_write_time(caminho_completo), permissao_real, tamanho_baixado};
    acesso_tabela.unlock();

    verifica_download.lock();
    esta_em_download = false;
    verifica_download.unlock();

    salvar_estado_local();

    return 1;
}
// ========================================================
// 3. O VIGIA (THREAD DE BACKGROUND)
// ========================================================
void sincronizador_automatico() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        bool precisa_salvar_json = false; 

        
        for (const auto& entrada : fs::directory_iterator(meu_diretorio)) {
            if (entrada.is_regular_file()) {
                std::string nome_arq = entrada.path().filename().string();

                if (nome_arq.front() == '.' || nome_arq.back() == '~') {
                    continue; 
                }

                bool arquivo_sendo_baixado = false;
                {
                    verifica_download.lock();
                    if (esta_em_download == true) {
                        arquivo_sendo_baixado = true;
                    }
                    verifica_download.unlock();
                }
                if (arquivo_sendo_baixado) {
                    continue; // Pula a análise desse arquivo completamente
                }

                auto tempo_atual = std::filesystem::last_write_time(entrada.path());
                int tamanho_fisico = (int)std::filesystem::file_size(entrada.path());

                bool arquivo_adicionado = false;
                bool foi_modificado = false;
                bool verificar_rede = false;
                int versao_local = 0;

                { 
                    std::lock_guard<std::mutex> lock_leitura(acesso_tabela);
                    if (meus_arquivos_estados.find(nome_arq) == meus_arquivos_estados.end()) {
                        arquivo_adicionado = true;
                    } else if (tempo_atual > meus_arquivos_estados[nome_arq].ultima_modificacao) {
                        foi_modificado = true;
                    } else {
                        verificar_rede = true;
                        versao_local = meus_arquivos_estados[nome_arq].versao;
                    }
                } 

                try {
                    if (arquivo_adicionado) {
                        std::cout << "\n[AUTO-SYNC] Novo arquivo detectado localmente: " << nome_arq << "\n";
                        int nova_v = tracker.call("registrar_peer", meu_usuario, nome_arq, privado, upload, tamanho_fisico).as<int>();
                        
                        std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                        meus_arquivos_estados[nome_arq] = {nova_v, tempo_atual, privado, tamanho_fisico};
                        precisa_salvar_json = true;
                    } 
                    else if (foi_modificado) {
                        std::cout << "\n[AUTO-SYNC] Mudanca local em " << nome_arq << " detectada!\n";
                        int nova_v = tracker.call("atualizar_arquivo", meu_usuario, nome_arq, tamanho_fisico).as<int>();
                        
                        if (nova_v == -2) {
                            std::cout << "[AUTO-SYNC] BLOQUEADO: Arquivo somente leitura ou privado!\n";
                            // A chamada abaixo já cuida de atualizar a memória quando o download acaba
                            fazer_download(nome_arq, tracker); 
                        } else {
                            std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                            meus_arquivos_estados[nome_arq].versao = nova_v;
                            meus_arquivos_estados[nome_arq].ultima_modificacao = tempo_atual;
                            meus_arquivos_estados[nome_arq].tamanho_bytes = tamanho_fisico; 
                        }
                        precisa_salvar_json = true;
                    }
                    else if (verificar_rede) {
                        int versao_rede = tracker.call("verificar_versao", meu_usuario, nome_arq).as<int>();
                        
                        if (versao_rede == -1) {
                            std::cout << "\n[AUTO-SYNC] O arquivo '" << nome_arq << "' foi morto na rede (Lapide). Removendo localmente...\n";
                            
                            std::filesystem::remove(entrada.path()); // Exclui do disco
                            
                            std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                            meus_arquivos_estados.erase(nome_arq); // Exclui do JSON
                            precisa_salvar_json = true;
                            
                            tracker.call("confirmar_remocao_seeder", meu_usuario, nome_arq); 
                            
                        }
                        else if (versao_rede > versao_local) {
                            std::cout << "\n[AUTO-SYNC] Baixando versao atualizada (v" << versao_rede << ") de " << nome_arq << "...\n";
                            fazer_download(nome_arq, tracker); 
                        }
                    }
                } catch (const std::exception& e) {}
            }
        }
        
        if (precisa_salvar_json) {
            salvar_estado_local();
        }
    }
}

// ========================================================
// 4. SERVIDOR DO CLIENTE & FUNÇÕES DO MENU
// ========================================================
std::vector<char> transferir_pagina(std::string nome_arquivo, int indice_pagina) {

    std::lock_guard<std::mutex> transferencia(adiquirir_pagina);

    std::string caminho_completo = meu_diretorio + "/" + nome_arquivo;
    std::ifstream arquivo(caminho_completo, std::ios::binary);
    
    if (!arquivo.is_open()) return std::vector<char>();

    arquivo.seekg(0, std::ios::end);
    std::streamsize tamanho_total = arquivo.tellg();
    
    std::streamsize offset = indice_pagina * PAGINA;
    std::streamsize bytes_restantes = tamanho_total - offset;
    std::streamsize tamanho_leitura = std::min((std::streamsize)PAGINA, bytes_restantes);

    if (tamanho_leitura <= 0) return std::vector<char>(); 

    arquivo.seekg(offset, std::ios::beg);
    std::vector<char> buffer(tamanho_leitura);
    
    if (arquivo.read(buffer.data(), tamanho_leitura)) {
        return buffer;
    }
    adiquirir_pagina.unlock();
    return std::vector<char>();
}

void excluir_obj() {
    std::string arquivo_requirido;
    std::cout << "Digite o nome do arquivo que deseja excluir:\n";
    std::cin >> arquivo_requirido;

    // A chamada RPC para o servidor. O Servidor fará a lógica:
    // - Se 'meu_usuario' for o dono: muda a versão do arquivo para -1.
    // - Se 'meu_usuario' NÃO for o dono: apenas remove ele da lista de peers deste arquivo.
    int status = tracker.call("solicitar_exclusao_arquivo", meu_usuario, arquivo_requirido).as<int>();

    if (status == 1) { // Sucesso na requisição
        std::cout << "[PEER] Solicitacao de exclusao processada pelo servidor.\n";
        
        std::string caminho_completo = meu_diretorio + "/" + arquivo_requirido;
        
        // Exclui fisicamente do disco local
        if (std::filesystem::exists(caminho_completo)) {
            std::filesystem::remove(caminho_completo);
        }

        // Limpa da memória local
        acesso_tabela.lock();
        if (meus_arquivos_estados.find(arquivo_requirido) != meus_arquivos_estados.end()) {
            meus_arquivos_estados.erase(arquivo_requirido);
        }
        acesso_tabela.unlock();
        
        salvar_estado_local();
    } else {
        std::cout << "[ERRO] Falha ao excluir. Arquivo nao existe ou erro no servidor.\n";
    }
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
    } while (permissao < 0 || permissao > 2); 
    
    permissao += 3;

    int tamanho = (int)std::filesystem::file_size(caminho_completo);

    int estado = tracker.call("registrar_peer", meu_usuario, arquivo, (int)permissao, (int)upload, (int)tamanho).as<int>();
    
    if (estado == 1) {
        std::cout << "Arquivo registrado com sucesso!\n";
        acesso_tabela.lock();
        auto tempo_mod = std::filesystem::last_write_time(caminho_completo);
        meus_arquivos_estados[arquivo] = {1, tempo_mod, permissao, tamanho}; 
        acesso_tabela.unlock(); 
       
        salvar_estado_local();
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
    if(fazer_download(arquivo_requirido, tracker) == 0){
        std::cout << "O download do arquivo requirido falhou\n";
        return;
    } 
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

    std::ofstream arquivo(caminho_completo); 
    if(arquivo.is_open()){
        arquivo << conteudo; 
        arquivo.close();
        std::cout << "[SISTEMA] Arquivo criado no disco!\n";
        
        int tamanho = (int)std::filesystem::file_size(caminho_completo);

        if (tracker.call("registrar_peer", meu_usuario, nome_arquivo, (int)permissao, (int)upload, (int)tamanho).as<int>() == 1) {
            std::cout << "[TRACKER] Arquivo registrado na rede!\n";
            
            std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
            meus_arquivos_estados[nome_arquivo] = {1, std::filesystem::last_write_time(caminho_completo), permissao, tamanho};
        }
        
        salvar_estado_local();
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

    carregar_estado_local();

    std::cout << "[SISTEMA] Sincronizando arquivos locais com o Servidor...\n";
    
    // Substituimos 'encontrou_arquivos_novos' por esta flag unificada
    bool precisa_salvar_json = false; 

    for (const auto& entrada : fs::directory_iterator(meu_diretorio)) {
        if (entrada.is_regular_file()) { 

            std::string nome_arq = entrada.path().filename().string();
            
            if (nome_arq.front() == '.' || nome_arq.back() == '~') {
                continue; 
            }

            int permissao_usada = privado; 
            int tamanho = (int)std::filesystem::file_size(entrada.path()); 
            int versao_json = 0;
            
            acesso_tabela.lock();
            bool conhecido = (meus_arquivos_estados.find(nome_arq) != meus_arquivos_estados.end());
            if (conhecido) {
                permissao_usada = meus_arquivos_estados[nome_arq].permissao;
                versao_json = meus_arquivos_estados[nome_arq].versao; // Pega a versão da nossa última sessão
            }
            acesso_tabela.unlock();

            if (!conhecido) {
                // Arquivo criado offline
                tracker.call("registrar_peer", meu_usuario, nome_arq, (int)permissao_usada, (int)upload, (int)tamanho).as<int>();
                int versao_rede = tracker.call("verificar_versao", meu_usuario, nome_arq).as<int>();
                auto tempo_mod = std::filesystem::last_write_time(entrada.path());
                
                acesso_tabela.lock();
                meus_arquivos_estados[nome_arq] = {versao_rede, tempo_mod, permissao_usada, tamanho}; 
                acesso_tabela.unlock();
                
                precisa_salvar_json = true;
                std::cout << " -> [" << nome_arq << "] Novo arquivo monitorado na rede.\n";
            } else {
                int versao_rede = tracker.call("verificar_versao", meu_usuario, nome_arq).as<int>();
                
                // === LÓGICA DE EXCLUSÃO CORRIGIDA (LAPIDE E GC) ===
                // Se a rede diz -1 (Lapide Ativa) OU a rede diz 0 mas nós tínhamos o arquivo (Lixo já Coletado)
                if (versao_rede == -1 || (versao_rede == 0 && versao_json > 0)) {
                    std::cout << "\n[INIT-SYNC] O arquivo '" << nome_arq << "' foi excluido da rede enquanto voce estava offline. Removendo localmente...\n";
                            
                    std::filesystem::remove(entrada.path()); // Exclui do disco
                            
                    std::lock_guard<std::mutex> lock_escrita(acesso_tabela);
                    meus_arquivos_estados.erase(nome_arq); // Exclui do JSON
                    precisa_salvar_json = true;
                    
                    // Só avisa a saída da lista de seeders se a Lapide ainda existir no servidor
                    if (versao_rede == -1) {
                        tracker.call("confirmar_remocao_seeder", meu_usuario, nome_arq); 
                    }
                }
                // ====================================================
                else if (versao_rede > versao_json) {
                    std::cout << "\n[INIT-SYNC] O arquivo '" << nome_arq << "' foi alterado na rede enquanto voce estava offline!\n";
                    std::cout << "[INIT-SYNC] Baixando a atualizacao (v" << versao_json << " -> v" << versao_rede << ")...\n";
                    
                    // Fazer o download vai atualizar a tabela JSON e a data física
                    fazer_download(nome_arq, tracker); 
                } else {
                    tracker.call("registrar_peer", meu_usuario, nome_arq, (int)permissao_usada, (int)upload, (int)tamanho).as<int>();
                    std::cout << " -> [" << nome_arq << "] Sincronizado e online (v" << versao_json << ").\n";
                }
            }
        }
    }
    
    if (precisa_salvar_json) {
        salvar_estado_local();
    }
}

void prova_vida(){
   while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        try {
            // Sinal de vida imune a travamentos de I/O
            tracker.call("heartbeat",meu_usuario,meu_ip_atual,minha_porta); 
            
            // Opcional: Manter a verificação de IP dinâmico aqui também é uma boa prática
            std::string ip_verificado = obter_meu_ip();
            if (ip_verificado != meu_ip_atual) {
                tracker.call("atualizar_ip_usuario", meu_usuario, ip_verificado);
                meu_ip_atual = ip_verificado;
            }
        } catch (...) {
            // Se o servidor cair, o heartbeat silencia até ele voltar
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
    meu_ip_atual = obter_meu_ip(); 

    std::cout << "[SISTEMA] Inicializando com IP Local: " << meu_ip_atual << "\n";

    tracker.call("registrar_usuario_rede", meu_usuario, meu_ip_atual, minha_porta);

    inicializar_sistema_de_arquivos();

    rpc::server srv(minha_porta);
    srv.bind("transferir_pagina", &transferir_pagina);
    srv.async_run(10); 
    std::cout << "[PEER] Escutando pedidos na porta " << minha_porta << "...\n";

    std::thread thread_udp(servidor_udp_ping, minha_porta);
    thread_udp.detach();
    std::cout << "[PEER] Escutando pings UDP na porta " << (minha_porta + 10000) << "...\n";

    std::thread thread_sync(sincronizador_automatico); 
    thread_sync.detach(); 

    std::thread thread_hertbeat(prova_vida);
    thread_hertbeat.detach();

    int op = 0; 
    while(op != -1){
        std::cout << "\n1: Cadastrar arquivo | 2: Baixar arquivo | 3: Criar arquivo | 4: Excluir arquivo | -1: Sair\nEscolha: ";
        std::cin >> op;

        switch (op) {
            case -1: std::cout << "Encerrando peer...\n"; break;
            case 1: cadastra_obj(); break;
            case 2: aquisicao_obj(); break;
            case 3: criar_arquivo_txt(); break;
            case 4: excluir_obj(); break;
            default: std::cout << "Opcao invalida.\n"; break;
        }
    }
    
    return 0; 
}