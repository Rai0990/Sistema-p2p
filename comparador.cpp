#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <cstring>
#include <chrono>

namespace fs = std::filesystem;

bool comparar_arquivos(const std::string& caminho1, const std::string& caminho2) {
    // 1. Verificação Rápida: Tamanho Físico
    auto tamanho1 = fs::file_size(caminho1);
    auto tamanho2 = fs::file_size(caminho2);

    if (tamanho1 != tamanho2) {
        std::cout << "[FALHA] Os tamanhos sao diferentes!\n";
        std::cout << " -> Arquivo 1: " << tamanho1 << " bytes\n";
        std::cout << " -> Arquivo 2: " << tamanho2 << " bytes\n";
        return false;
    }

    std::cout << "[OK] Tamanhos exatos (" << tamanho1 << " bytes). Iniciando leitura binaria profunda...\n";

    // 2. Abertura Binária
    std::ifstream arquivo1(caminho1, std::ios::binary);
    std::ifstream arquivo2(caminho2, std::ios::binary);

    if (!arquivo1.is_open() || !arquivo2.is_open()) {
        std::cout << "[ERRO] Falha ao abrir os arquivos para leitura.\n";
        return false;
    }

    // 3. Comparação em Chunks de 1 MB (Para não explodir a RAM com arquivos de 2GB)
    const size_t TAMANHO_BUFFER = 1024 * 1024; 
    std::vector<char> buffer1(TAMANHO_BUFFER);
    std::vector<char> buffer2(TAMANHO_BUFFER);

    long long bytes_lidos = 0;

    do {
        arquivo1.read(buffer1.data(), TAMANHO_BUFFER);
        arquivo2.read(buffer2.data(), TAMANHO_BUFFER);

        std::streamsize lidos1 = arquivo1.gcount();
        std::streamsize lidos2 = arquivo2.gcount();

        if (lidos1 != lidos2 || std::memcmp(buffer1.data(), buffer2.data(), lidos1) != 0) {
            std::cout << "\n[FALHA FATAL] Corrupcao de dados detectada na marca de " << bytes_lidos << " bytes!\n";
            return false;
        }

        bytes_lidos += lidos1;
    } while (arquivo1.good() || arquivo2.good());

    return true;
}

int main() {
    std::string arquivo_original, arquivo_baixado;

    std::cout << "=========================================\n";
    std::cout << "       VERIFICADOR DE INTEGRIDADE P2P    \n";
    std::cout << "=========================================\n\n";

    std::cout << "Digite o caminho do arquivo ORIGINAL (Ex: ./pasta_Rai/matriz.txt): \n> ";
    std::cin >> arquivo_original;

    std::cout << "Digite o caminho do arquivo BAIXADO (Ex: ./pasta_Cliente2/matriz.txt): \n> ";
    std::cin >> arquivo_baixado;

    if (!fs::exists(arquivo_original) || !fs::exists(arquivo_baixado)) {
        std::cout << "[ERRO] Um ou ambos os arquivos nao foram encontrados no disco!\n";
        return 1;
    }

    auto inicio = std::chrono::high_resolution_clock::now();

    bool sao_identicos = comparar_arquivos(arquivo_original, arquivo_baixado);

    auto fim = std::chrono::high_resolution_clock::now();
    auto tempo_ms = std::chrono::duration_cast<std::chrono::milliseconds>(fim - inicio).count();

    std::cout << "\n=========================================\n";
    if (sao_identicos) {
        std::cout << " [SUCESSO ABSOLUTO] Os arquivos sao CLONES EXATOS!\n";
        std::cout << " Nenhum byte foi corrompido ou perdido na rede P2P.\n";
    } else {
        std::cout << " [ALERTA] Os arquivos sao DIFERENTES!\n";
    }
    std::cout << " Tempo de verificacao: " << tempo_ms << " ms\n";
    std::cout << "=========================================\n";

    return 0;
}