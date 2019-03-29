package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-oci8"
)

/**
  Variaveis do sistema
  @user usuario do banco de dados
  @password senha do banco de dados
  @ip ip do banco de dados
  @likeReport nome do relatorio que sera feito alteração
  @dbName Sid do banco de dados
  @searchRegex paremetro de busca dentro dos relatorios
  @replace informa se sera feito ou n substituição de valores dentro do relarorio
  @replaceValue valor que será alterado dentro do relatorio
  @wg sincronização de grupos
  @scanner leitor de linha de comando
**/
var (
	user         string
	password     string
	ip           string
	likeReport   string
	dbName       string
	searchRegex  string
	replace      int
	replaceValue string
	wg           sync.WaitGroup
	scanner      = bufio.NewScanner(os.Stdin)
)

/**
  Estrutura dos arquivos
**/
type arquivo struct {
	Nome      string `xml:"nome"`
	Nurfe     int    `xml:"nurfe"`
	Sequencia int    `xml:"sequencia"`
	Conteudo  []byte `xml:"conteudo"`
}

/**
  Estrutura do parceiro
**/
type parceiro struct {
	Owner string
}

func main() {
	exibeIntroducao()
	leVariaveis()
}

func exibeIntroducao() {
	fmt.Println("Inicializando...")
	fmt.Println()
	fmt.Println(`  _____                                  _ `)
	fmt.Println(` |  __ \                                | | `)
	fmt.Println(` | |__) |   ___   _ __     ___    _ __  | |_ `)
	fmt.Println(` |  _  /   / _ \ | '_ \   / _ \  | '__| | __| `)
	fmt.Println(` | | \ \  |  __/ | |_) | | (_) | | |    | |_  `)
	fmt.Println(` |_|  \_\  \___| | .__/   \___/  |_|     \__| `)
	fmt.Println(`                 | |                         `)
	fmt.Println(`                 |_|                         `)
	fmt.Println()

}

/**
  Le as valores de perguntas para rodar programa
**/
func leVariaveis() {
	var loop = 1
	for loop == 1 {

		fmt.Println("Informe o IP do banco de dados..")
		fmt.Scan(&ip)
		fmt.Println("Informe o SID do banco de dados..")
		fmt.Scan(&dbName)
		fmt.Println("Informe o USER do banco de dados..")
		fmt.Scan(&user)
		fmt.Println("Informe o Password do banco de dados")
		fmt.Scan(&password)

		fmt.Println("Informe nome do relatorio que deseja alterar")

		if scanner.Scan() {
			likeReport = scanner.Text()
		} else {
			fmt.Println("Finalizando...")
			loop = 0
		}

		fmt.Println("Informe o paremetro de Busca que deseja alterar dentro do relatorio")

		if scanner.Scan() {
			searchRegex = scanner.Text()
		} else {
			fmt.Println("Finalizando...")
			loop = 0
		}

		fmt.Println("Deseja substituir?")
		fmt.Println("1- Sim")
		fmt.Println("2- Não")

		fmt.Scan(&replace)

		if replace == 1 {
			fmt.Println("Por qual Valor?")
			if scanner.Scan() {
				replaceValue = scanner.Text()
			} else {
				fmt.Println("Finalizando...")
				loop = 0
			}
			fmt.Println(replaceValue)

			startSearch()
		} else {
			fmt.Println("Finalizando...")
			loop = 0
		}

		time.Sleep(5)
		clear()
		fmt.Println("Deseja verificar outro banco ? ")
		fmt.Println("1- Sim")
		fmt.Println("2- Não")

		fmt.Scan(&loop)

	}

}

/**
  Limpa o terminal
**/
func clear() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

/**
  Inicia a conexão com banco de dados, pega os Parceiros com a tabela TSIRFA
  para procurar possiveis erros
**/
func startSearch() {
	connection, err := newConnection()
	if err != nil {
		log.Fatalln("Erro ao criar conexão com Banco de Dados")
		panic(err)
	}

	parceiros := getParceiros(connection)

	if parceiros == nil {
		log.Fatalln("Nenhum parceiro encontrado")
	}

	for _, parceiro := range parceiros {
		updateSession(connection, parceiro.Owner)
		browseErrorsReports(connection, parceiro.Owner)
	}

}

/**
  Atualiza a sessão baseado no parceiro atual
  @connection conexão com banco de dados
  @parceiro parceiro atual que sera alterado na sessão
**/
func updateSession(connection *sql.DB, parceiro string) {
	fmt.Println(parceiro)
	statement := fmt.Sprintf("ALTER SESSION SET current_schema = %s", parceiro)
	fmt.Println(statement)
	_, err := connection.Exec(statement)

	if err != nil {
		log.Fatalln("Erro mudar sessão", err)
		panic(err)
	}
}

/**
  Cria conexão com banco de daodos
**/
func newConnection() (*sql.DB, error) {
	dbUrl := ip + "/" + dbName

	stringConnection := fmt.Sprintf("%s:%s@%s", user, password, dbUrl)
	fmt.Println("Base:", stringConnection)
	return sql.Open("oci8", stringConnection)
}

/**
  Pega todos os parceiros com a tabela TSIRFA
  @connection conexão com banco de dados
**/
func getParceiros(connection *sql.DB) []parceiro {
	rows, err := connection.Query("SELECT OWNER FROM DBA_TABLES WHERE TABLE_NAME = 'TSIRFA'")

	if err != nil {
		log.Fatalln("Erro ao buscar parceiros, Erro:\n", err)
		panic(err)
	}

	defer rows.Close()

	parceiros := []parceiro{}

	for rows.Next() {
		var parc parceiro
		if err := rows.Scan(&parc.Owner); err != nil {
			log.Println("Erro ao buscar parceiros, Erro:\n", err)
			return nil
		}
		parceiros = append(parceiros, parc)
	}

	return parceiros
}

/**
  Procura erros no relatorio do parceiro a sessão
  @connection conexão com banco de dados
  @parceiro parceiro atual que sera alterado na sessão
**/
func browseErrorsReports(connection *sql.DB, parceiro string) {
	arquivos := getReports(connection)

	if arquivos == nil {
		log.Println("Não foi encontrado nenhum relatório neste parceiro.")
		os.Exit(0)
	}

	for _, arquivo := range arquivos {
		log.Println("Lendo relatório", arquivo.Nome)
		wg.Add(1)
		if arquivo.Conteudo != nil {
			go searchRegexAndReplace(arquivo, connection)
		}
	}

	wg.Wait()

	log.Println("Finalizando arquivos do parceiro ", parceiro)
}

/**
  Pega todos relatorios do parceiro da sessão
  @connection conexão com banco de dados
**/
func getReports(connection *sql.DB) []arquivo {

	rows, err := connection.Query("SELECT NURFE, SEQUENCIA, NOME, ARQUIVOBIN FROM TSIRFA WHERE NOME = :1", likeReport)

	if err != nil {
		log.Fatalln("Erro ao buscar reports, Erro:\n", err)
		panic(err)
	}

	defer rows.Close()

	arquivos := []arquivo{}

	for rows.Next() {
		var arq arquivo
		if err := rows.Scan(&arq.Nurfe, &arq.Sequencia, &arq.Nome, &arq.Conteudo); err != nil {
			log.Println("Erro ao buscar reports, Erro:\n", err)
			return nil
		}
		arquivos = append(arquivos, arq)
	}

	return arquivos
}

/**
  Procura o @searchRegex dentro dos relatorios e substitui caso exista
  @arq arquivo jrxml que sera feito a busca do @searchRegex
  @connection conexão com banco de dados
**/
func searchRegexAndReplace(arq arquivo, connection *sql.DB) {
	defer wg.Done()

	conteudo := string(arq.Conteudo)
	conteudo = strings.ReplaceAll(conteudo, searchRegex, replaceValue)

	if len(conteudo) > 0 {
		arq.Conteudo = []byte(conteudo)
		updateReport(arq, connection)
	}

}

/**
  Atualiza o relatorio com valor @replaceValue que foi substituido
  @arq arquivo jrxml atualizado que sera salvo no banco de dados do parceiro da sessão
  @connection conexão com banco de dados
**/
func updateReport(arq arquivo, connection *sql.DB) {

	defer connection.Close()

	_, err := connection.Exec("UPDATE TSIRFA SET ARQUIVOBIN = :1 WHERE NURFE = :2 AND SEQUENCIA = :3", arq.Conteudo, arq.Nurfe, arq.Sequencia)

	if err != nil {
		panic(err)
	}

	log.Println("Arquivo ", arq.Nome, "com nurfe", arq.Nurfe, "alterado com sucesso!")

}
