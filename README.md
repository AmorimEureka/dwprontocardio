Pipeline de Extração Tratamento de Dados
=============================================================

<br>
 O projeto visa a criação e implantação do Data Warehouse do Hospital Prontocardio.
<br>
<br>
<br>


 Configurações:
 =============

<br>

<details open>
  <summary>
    <strong>TERMINAL:</strong>
  </summary>

<br>
  - Windows Terminal: É interessante a instalação e configuração do win terminal, pois permite rodar terminais em abas, alterar cores e temas, configurar atalhos e muito mais. O processo é simples e pode ser realizado pela loja de aplicativos da Microsoft. Na sequência é interessante torna-lô terminal padrão.
</details>

<br>
<br>

<details open>
  <summary>
    <strong>WSL:</strong>
  </summary>

<br>

  - Windows Subsystem for Linux - WSL 2: O WSL 2 permite execução completa do kernel Linux no windows permitindo melhor desempenho para acessar arquivos e compatibilidade completa de chamada do sistema, além da possibilidade de utilizar Docker nativo (pré-requisito do nosso projeto).

- O  WSL 2 tem acesso quase que total ao recursos de sua máquina:
    * A 1TB de disco rígido. É criado um disco virtual de 1TB para armazenar os arquivos do Linux (este limite pode ser expandido, ver a área de dicas e truques).
    * A usar completamente os recursos de processamento.
    * A usar 50% da memória RAM disponível.
    * A usar 25% da memória disponível para SWAP (memória virtual).

    _\* Se você quiser personalizar estes limites, crie um arquivo chamado `.wslconfig` na raiz da sua pasta de usuário `(C:\Users\<seu_usuario>)` e defina estas configurações:_

    `[wsl2]`
    <br>

    `memory=8GB`
    <br>

    `processors=4`
</details>

<br>
<br>

<details open>
  <summary>
    <strong>DOCKER ENGINE:</strong>
  </summary>

<br>



1 - **ANTES DE INSTALAR, VERIFICAR INSTALAÇÕES DE TERCEIROS E REMOVER:**


```sh
    for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done
```
<br>

2 - **O COMANDO ACIMA IRÁ DESINSTALAR OS SEGUINTES PACKAGES:**
* docker.io
* docker-compose
* docker-compose-v2
* docker-doc
* podman-docker

<br>

3 - **INSTALAÇÃO COM**  ['apt repository'](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) :


```sh
    -- Adicione a chave GPG oficial do Docker
    sudo apt-get update
    sudo apt-get install ca-certificates curl
    sudo install -m 0755 -d /etc/apt/keyrings
    sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    sudo chmod a+r /etc/apt/keyrings/docker.asc

    -- Adicione o repositório ao Apt sources
    echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update

```

<br>

> [!IMPORTANT]
> Por padrão o grupo de usuários do Docker é criado, porém sem usuários vinculados. <br>
> Nesse caso para executar comandos docker será necessário usar a palavra reservada `sudo` ou siga as instruções abaixo pos-install

<br>

4 -  **POS INSTALL:**

- <u>Criar grupo de usuários docker</u>:
    ``` bash
        sudo groupadd docker
    ```
- <u>Adicionar o usuário ao grupo:</u>
    ``` bash
        sudo usermod -aG docker $USER
    ```
    - _Encerrer a sessão e volte novamente para que a associação seja reavaliada_

- <u>Ativar as alterações de grupo:</u>
    ``` bash
        newgrp docker
    ```

- <u>Verificar se a configuração foi bem sucedida sem _'sudo'_ :</u>

    ``` bash
        docker run hello-world
    ```

<br>

[Link Configuração Pós Instalação Docker Engine](https://docs.docker.com/engine/install/linux-postinstall/)


</details>

<br>
<br>

<details open>
  <summary>
    <strong>ASTRO:</strong>
  </summary>

<br>

1 - **INSTALAÇÃO**:



```sh
curl -sSL install.astronomer.io | sudo bash -s
```

<br>

2 - **ATUALIZAR**:


```
curl -sSL install.astronomer.io | sudo bash -s
```

<br>

3 - **DESINSTALAR**:


```sh
sudo rm /usr/local/bin/astro
```

<br>

4 - **INICIAR PROJETO ASTRO**:


```sh
mkdir <nome_do_seu_projeto_astro>
cd <nome_do_seu_projeto_astro>
astro dev init
```

<br>

5 - **O DIRETÓRIO DO PROJETO TERÁ A SEGUINTE ESTRUTURA:**

```
.
├── .env # Arquivo contendo suas variáveis de ambiente
├── dags # Onde seus arquivos de DAG's devem estar
│   ├── extracao_suprimentos_MV.py # Exemplos dag's
│   └── maestro_prontocardio.py # Exemplos dag's
├── Dockerfile # Dockerfile para apontar volumes, variáveis de ambiente e outras substituíções.
├── include # Para outros scripts
├── plugins # Plugins customizados
│   └── example-plugin.py
├── tests # Scripts para testar as dag's
│   └── test_dag_example.py
├── airflow_settings.yaml # Conexões, variáveis e pools do airflow
├── packages.txt # Packages OS-level
└── requirements.txt # Packages python
```
</details>

<br>
<br>

<details open>
  <summary>
    <strong>DBT-CORE:</strong>
  </summary>

<br>

1 - **CRIAR AMBIENTE VIRTUAL**:

```sh
mkdir <nome_do_seu_projeto_dbt>
cd <nome_do_seu_projeto_dbt>
python -m venv dbt-env				# create the environment
```

<br>

2 - **ATIVAR AMBIENTE VIRTUAL**:

```sh
source dbt-env/bin/activate			# activate the environment for Mac and Linux OR
dbt-env\Scripts\activate			# activate the environment for Windows
```
<br>

3 - **INSTALAR ADAPTER DO POSTGRE**:

```sh
python -m pip install dbt-core dbt-postgres
```

<br>

4 - **VALIDANDO INSTALAÇÃO**:

```sh
$ dbt --version
installed version: 1.0.0
latest version: 1.0.0

Up to date!

Plugins:
- postgres: 1.0.0
```

<br>

5 - **INICIANDO NOVO PROJETO**:

```sh
dbt init
```

<br>

6 - **TESTAR PROJETO**:

```sh
dbt debug
```

<br>

7 - **O DIRETÓRIO DO PROJETO TERÁ A SEGUINTE ESTRUTURA:**

```sh
.
├── README.md
├── analyses
├── seeds
│   └── employees.csv
├── dbt_project.yml
├── macros
│   └── customizar_schemas_default.sql
├── models
│   ├── intermediate
│   │   └── suprimentos
│   │       ├── _schema.yml
│   │       └── _int_solicitacao.sql
│   ├── marts
│   │   ├── suprimentos
│   │   │   ├── schema.yml
│   │   │   ├── pedidos.sql
│   │   │   └── solicitacao.sql
│   │   └── marketing
│   │       ├── _schema.sql
│   │       └── _marketing.sql
│   ├── staging
│   │   ├── suprimentos
│   │   │   ├── _suprimentos__docs.md
│   │   │   ├── _stg_pedidos.sql
│   │   │   ├── _stg_solicitacao.sql
│   │   │   ├── base
│   │   │   │   ├── suprimentos__fornecedores.sql
│   │   │   │   └── suprimentos__deleted_fornecedores.sql
│   │   │   ├── suprimentos__pedidos.sql
│   │   │   └── suprimentos__solicitacao.sql
│   │   └── stripe
│   │       ├── _stripe__models.yml
│   │       ├── _stripe__sources.yml
│   │       └── stg_stripe__payments.sql
│   └── utilities
│       └── all_dates.sql
├── packages.yml
├── snapshots
└── tests
    └── testes.sql
```

</details>

















