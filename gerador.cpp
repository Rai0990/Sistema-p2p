#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib>
#include <ctime>
#include <filesystem>

int main() {
    std::string nome_arquivo;
    long long linhas, colunas; // Usando long long para permitir matrizes colossais

    std::cout << "=========================================\n";
    std::cout << "   GERADOR DE MATRIZ (STRESS TEST P2P)   \n";
    std::cout << "=========================================\n\n";

    std::cout << "Digite o nome do arquivo final (ex: carga_pesada.txt): ";
    std::cin >> nome_arquivo;

    std::cout << "Digite o numero de linhas: ";
    std::cin >> linhas;

    std::cout << "Digite o numero de colunas: ";
    std::cin >> colunas;

    std::cout << "\n[SISTEMA] Iniciando a geracao dos dados...\n";

    // Inicia a contagem de tempo
    auto inicio = std::chrono::high_resolution_clock::now();

    std::ofstream arquivo(nome_arquivo);
    if (!arquivo.is_open()) {
        std::cerr << "[ERRO] Falha ao criar o arquivo no disco!\n";
        return 1;
    }

    // Alimenta a semente randômica
    srand(time(NULL));

    for (long long i = 0; i < linhas; ++i) {
        for (long long j = 0; j < colunas; ++j) {
            // Gera números de 0 a 999 separados por espaço
            arquivo << (rand() % 1000) << " ";
        }
        // Quebra de linha no final de cada linha da matriz
        arquivo << "\n"; 
        
        // Feedback visual para matrizes muito grandes
        if (i > 0 && i % 1000 == 0) {
            std::cout << " -> " << i << " linhas geradas...\n";
        }
    }

    arquivo.close();

    auto fim = std::chrono::high_resolution_clock::now();
    auto tempo_gasto = std::chrono::duration_cast<std::chrono::milliseconds>(fim - inicio).count();

    // Calcula o peso do arquivo gerado
    long long tamanho_bytes = std::filesystem::file_size(nome_arquivo);
    double tamanho_mb = tamanho_bytes / (1024.0 * 1024.0);

    std::cout << "\n=========================================\n";
    std::cout << "[SUCESSO] Matriz gerada e salva no disco!\n";
    std::cout << "-> Arquivo: " << nome_arquivo << "\n";
    std::cout << "-> Tempo de processamento: " << tempo_gasto << " ms\n";
    std::cout << "-> Peso final: " << tamanho_bytes << " bytes (Aprox. " << tamanho_mb << " MB)\n";
    std::cout << "=========================================\n";

    return 0;
}