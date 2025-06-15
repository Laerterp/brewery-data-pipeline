Hackathon2025
Repositório do projeto proposto no Hackathon 2025 da EmpregaDados
🚀 Objetivo
Construir um pipeline ETL que:

Extraia dados brutos de diferentes fontes (CSV);
Faça o tratamento e padronização das colunas (camada Silver);
Entregue os dados prontos para visualização e análises (Gold);
Geração de dashboards para tomada de decisões;
Garanta rastreabilidade, escalabilidade e reprodutibilidade.
🧪 Como Rodar
Clone o repositório:
git clone https://github.com/AlvaroBernardino/hackathon2025
cd hackathon2025
Instale as dependências:
pip install -r requirements.txt
Execute os notebooks
Execute os notebooks da pasta "etl" na ordem numérica
Verifique os bancos de dados locais
Utilize os scripts na pasta "tests" para verificar os bancos de dados através de "SELECT"
🧪 Diretórios
config: Arquivos de configuração, credenciais
database: Arquivos de bancos de dados
etl: Scripts de ETL
mkdown: Arquivos de texto que podem ser úteis
modelagem: Arquivos .dbml para visualização fácil dos schemas
retired: Scripts e snippets de legado, que não serão utilizados no pipeline
changelog.md: Histórico de modificações. Nas mensagens dos commits, colocar simplesmente o nome da versão (Ex.: 0.2.0) e colocar as mudanças neste arquivo.
✍️ Autores
Álvaro Bernardino
Caio Prates
Diego Simon
Laerte Rocha Neves
Luiz Henrique Popoff