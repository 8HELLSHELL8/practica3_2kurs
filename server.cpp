//FOR Programm
#include <iostream>
#include "headers/LinkedList.h"
#include "headers/HashTable.h"
#include <cmath>
#include <string>

//FOR SERVER
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <cstring>
#include <arpa/inet.h>


//FOR API
#include <boost/beast.hpp>
#include <boost/asio.hpp>
#include <boost/json.hpp>
#include <memory>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <algorithm>

//FOR multitreading
#include <thread>
#include <mutex>

//FOR using files in system
#include <filesystem>

//FOR Parsing
#include <cjson/cJSON.h>
#include <fstream>
#include <sstream>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace json = boost::json;
using namespace std;
using tcp = net::ip::tcp;

mutex mtx;
mutex menu_mtx;
mutex delMutex;
mutex insertMutex;
mutex selectMutex;



string getLastFolderName(const string& path) {

    size_t lastSlashPos = path.rfind('/');

    if (lastSlashPos != std::string::npos) {
        return path.substr(lastSlashPos + 1);
    }
    
    return path;
}

void unlockTable(const string& pathToDir)
{
    
    string tableName = getLastFolderName(pathToDir);
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 0;
    lockFile.close();
}

void lockTable(const string& pathToDir)
{
    
    string tableName = getLastFolderName(pathToDir);
    ofstream lockFile(pathToDir + "/" + tableName + "_lock");
    lockFile << 1;
    lockFile.close();
}

void increasePKSEQ(const string& tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int increasedLinesAmount = stoi(fileInput) + 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_pk_sequence");
    PKSEQupload << increasedLinesAmount;
    PKSEQupload.close();
    
}

void decreasePKSEQ(const string& tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    ifstream PKSEQread(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQread.is_open())
    {
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQread, fileInput);
    PKSEQread.close();

    int decreasedLinesAmount = stoi(fileInput) - 1;

    ofstream PKSEQupload(pathToDir + "/" + tableName + "_pk_sequence");
    PKSEQupload << decreasedLinesAmount;
    PKSEQupload.close();
}

string readJSON(const string& fileName) //Reading json content in string line
{ 
    fstream file(fileName);
    if (!file.is_open())
    {
        throw runtime_error("Error opening " + fileName + ".json file!");
    } 

    stringstream buffer;
    buffer << file.rdbuf();
    file.close();

    return buffer.str();
}

bool createDir(const string& dirName)
{
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + dirName;
    if (filesystem::create_directory(pathToDir)) return true;
    else return false;
}

void createFilesInSubFolder(const cJSON* table, const cJSON* structure, const string& subName)
{
    

    LinkedList<string> columnNames; 
    cJSON* tableArray = cJSON_GetObjectItem(structure, table->string); //Reading table column names
    int arrSize = cJSON_GetArraySize(tableArray); //Reading amount of columns in table

    for (size_t i = 0; i < arrSize; i++) //Insert column names in table
    {
        cJSON* arrayItem = cJSON_GetArrayItem(tableArray, i);
        columnNames.addtail(arrayItem->valuestring);
    }
    string pathToDir = filesystem::current_path();
    pathToDir += subName;
    
    ofstream CSV(pathToDir + "/1.csv"); //Create and fill up .csv table
    for (size_t i = 0; i < columnNames.size(); i++)
    {   
        if (i < columnNames.size()-1)
        {
            CSV << columnNames.get(i) << ",";
        }
        else
        {
            CSV << columnNames.get(i);
        }
        
    }
    CSV << endl;
    CSV.close();

    string pathToDirPQ = pathToDir + "/" + table->string + "_pk_sequence";
    ofstream PKSEQ(pathToDirPQ); //Creating file-counter for each table
    PKSEQ << "1";
    PKSEQ.close();

    unlockTable(pathToDir);
}

int getPKSEQ(string tableName)
{
    string pathToDir = filesystem::current_path(); //Getting table name
    pathToDir += "/" + tableName;

    string fileInput;
    
    ifstream PKSEQ(pathToDir + "/" + tableName + "_pk_sequence"); //Opening line counter
    if (!PKSEQ.is_open())
    {
        cout << pathToDir + "/" + tableName + "_pk_sequence";
        throw runtime_error("Error opening pk_sequence and reading it");
    }
    getline(PKSEQ, fileInput);
    PKSEQ.close();

    return stoi(fileInput);
}

LinkedList<string> getColumnNamesFromTable(string tableName)
{
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;

    ifstream COLNAMES(pathToDir + "/1.csv");
    string fileInput;
    getline(COLNAMES, fileInput, '\n');
    
    LinkedList<string> columnNames;
    string word = "";
    for (auto symbol : fileInput) //GETTING column names
    {
        if (symbol == ',')
        {
            columnNames.addtail(word);
            word = "";
            continue;
        }
        word += symbol;
    }
    if (!word.empty()) columnNames.addtail(word);
    COLNAMES.close();

    return columnNames;
}

LinkedList<HASHtable<string>> getTableLines(const string& tableName)
{
    
    LinkedList<HASHtable<string>> thisTable;
    int amountOfLinesInTable = getPKSEQ(tableName);
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    
    int filesCounter = ceil(static_cast<double>(amountOfLinesInTable)/1000); //Counting amount of .csv files
    for (int i = 0; i < filesCounter; i++)
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, amountOfLinesInTable);
        string fileInput;

        string pathToDir = filesystem::current_path();
        pathToDir += "/" + tableName;

        ifstream CSV(pathToDir + "/" + to_string(i+1) + ".csv");
        for (int row = startRow; row < endRow; row++)
        {
            getline(CSV, fileInput, '\n');
            HASHtable<string> tableLine(columnNames.size());
            string word = "";
            int wordCounter = 0;
            for (auto symbol : fileInput) //Process line
            {
                if (symbol == ',')
                {
                    tableLine.HSET(columnNames.get(wordCounter), word);
                    word = "";
                    wordCounter++;
                    continue;
                }
                word += symbol;
            }
            if (!word.empty()) tableLine.HSET(columnNames.get(wordCounter), word);
            thisTable.addtail(tableLine);
        }
        CSV.close();
    }
    
    return thisTable;
}

LinkedList<HASHtable<string>> readTable(const string& tableName)
{
    LinkedList<HASHtable<string>> thisTable;
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;

    int amountLines = getPKSEQ(tableName);
    int fileCount = ceil(static_cast<double>(amountLines) / 1000);
    
    for (int i = 0; i < fileCount; ++i) //Creating CSV if >1000 elements
    {
        fstream fileCSV(pathToDir + "/" + to_string(i+1) + ".csv");
        if (!fileCSV.good())
        {
            ofstream newFile(pathToDir + "/" + to_string(i+1) + ".csv");
            newFile.flush();
            newFile.close();
        }
        fileCSV.flush();
        fileCSV.close();
    }

    thisTable = getTableLines(tableName);
    return thisTable;
}

void uploadTable(LinkedList<HASHtable<string>> table, string tableName)
{
    int linesAmount = getPKSEQ(tableName);
    int fileCount = ceil(static_cast<double>(linesAmount) / 1000);
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    string pathToDir = filesystem::current_path();
    pathToDir += "/" + tableName;
    for (int i = 0; i < fileCount; ++i) 
    {
        int startRow = i * 1000;
        int endRow = min(startRow + 1000, linesAmount);
        ofstream UPLOAD(pathToDir + "/" + to_string(i + 1) + ".csv", ios::out | ios::trunc);
        if (!UPLOAD.is_open()) throw runtime_error("Error opening csv for table upload");
        for (int row = startRow; row < endRow; row++)
        {
            for (int column = 0; column < columnNames.size(); column++)
            {
                auto currentRow = table.get(row);

                if (column == columnNames.size() - 1)
                {
                    UPLOAD << currentRow.HGET(columnNames.get(column));
                    UPLOAD << "\n";
                }
                else
                {
                    UPLOAD << currentRow.HGET(columnNames.get(column));
                    UPLOAD << ",";
                }
                
            }
        }
        UPLOAD.flush();
        UPLOAD.close();
    }
    
}

void insert(LinkedList<string> values, string tableName)
{
    lock_guard<mutex> insertionMUTEX(insertMutex);
    lockTable(tableName);
    LinkedList<HASHtable<string>> table = readTable(tableName);
    
    LinkedList<string> columnNames = getColumnNamesFromTable(tableName);
    HASHtable<string> row(columnNames.size());
    if (values.size() == columnNames.size())
    {   
        for (int i = 0; i < columnNames.size(); i++)
        {
            row.HSET(columnNames.get(i),values.get(i));
        }
        table.addtail(row);
        increasePKSEQ(tableName);
    }
    else if (values.size() < columnNames.size())
    {
        for (int i = 0; i < columnNames.size(); i++)
        {
            if (i >= values.size())
            {
                row.HSET(columnNames.get(i),"EMPTY");
            }
            else
            {
                row.HSET(columnNames.get(i),values.get(i));
            }
        }
        table.addtail(row);
        increasePKSEQ(tableName);
    }
    else
    {
        unlockTable(tableName);
        throw runtime_error("Amount of values more than columns in table!");
    }

    uploadTable(table, tableName);

    unlockTable(tableName);
}

LinkedList<string> parseCommand(string userInput)
{
    LinkedList<string> dividedInput;
    string word = "";
    for (auto symbol : userInput)
    {
        if (symbol == '\'' || symbol == '(' || symbol == ')' || symbol == ' ' || symbol == ',')
        {
            if (!word.empty())
            {
                dividedInput.addtail(word);
            }
            word = "";
            continue;
        }
        word += symbol;
    }
    return dividedInput;
}

string parseTablenameForInsert(LinkedList<string> commandList)
{
    return commandList.get(2);
}

LinkedList<string> parseValuesForInsert(LinkedList<string> commandList)
{   
    LinkedList<string> values;
    if (commandList.get(3) != "VALUES")
    {
        throw runtime_error("Syntax error in pasing values for INSERT");
    }
    for (int i = 4; i < commandList.size(); i++)
    {
        values.addtail(commandList.get(i));
    }
    return values;
}

bool whereInside(LinkedList<string> commandList)
{
    return commandList.search("WHERE");
}

void handleINPUT(LinkedList<string> commandList)
{
    if (commandList.get(0) == "INSERT" && commandList.get(1) == "INTO" && whereInside(commandList) == 0)
    {
        LinkedList<string> values = parseValuesForInsert(commandList);
        string tableName = parseTablenameForInsert(commandList);
        insert(values, tableName);
    }
    else
    {
        throw runtime_error("Syntax error in input query!");
    }
}

bool isTableName(const string& element)
{
    for (auto sym : element)
    {
        if(sym == '.') return true;
    }
    return false;
}

string divideAndGetTable(const string& word)
{
    string tableName = "";
    for (auto sym : word)
    {
        if (sym == '.') return tableName;

        else tableName += sym;
    }
    return "";
}   

string divideAndGetColumn(const string& word)
{
    bool writeMode = 0;
    string tableName = "";
    for (auto sym : word)
    {
        if (writeMode == 1)
        {
            tableName += sym;
        }
        if (sym == '.') writeMode = 1;
    }
    return tableName;
}

LinkedList<string> getSelectedTablesFROM(LinkedList<string> commandList)
{
    LinkedList<string> selected;
    
    bool writeMode = 0;
    for (int i = 0; i < commandList.size(); i++)
    {
        auto token = commandList.get(i);
        if (token == "WHERE") break;
        if ( writeMode == 1) selected.addtail(token);
        if (token == "FROM") writeMode = 1;

    }
    
    return selected;
}

LinkedList<string> getSelectedTablesSELECT(LinkedList<string> commandList)
{
    LinkedList<string> selected;
    
    bool writeMode = 0;
    for (int i = 0; i < commandList.size(); i++)
    {
        auto token = commandList.get(i);
        if (token == "FROM") break;
        if ( writeMode == 1) selected.addtail(token);
        if (token == "SELECT") writeMode = 1;

    }
    
    return selected;
}

bool getFinalResult(LinkedList<bool> results, LinkedList<string> operators)
{
    bool finalRes;
    if (operators.size() == 0) return results.get(0);
    else
    {
         for (int i = 0; i < results.size() - 1; i++)
        {
            if (i == 0) finalRes = results.get(0);
            if (operators.get(i)== "AND")
            {
                finalRes = finalRes && results.get(i + 1);
            }
            else if (operators.get(i)== "OR")
            {
                finalRes = finalRes || results.get(i + 1);
            }
        }
    }
   
    return finalRes;
}

bool checkCondition(string table1Name, HASHtable<string> row1, 
                    string table2Name, HASHtable<string> row2, 
                    LinkedList<string> conditions, LinkedList<string> operators)
{

    LinkedList<bool> results;


    for (int i = 0; i < conditions.size(); i += 3)
    {
        string left = conditions.get(i);
        string op = conditions.get(i + 1);
        string right = conditions.get(i + 2);

        if (isTableName(left) && op == "=")
        {
            if (divideAndGetTable(left) ==  table1Name)
            {
                left = row1.HGET(divideAndGetColumn(left));
            }
            else if (divideAndGetTable(left) ==  table2Name)
            {
                left = row2.HGET(divideAndGetColumn(left));
            }
            else
            {
                throw runtime_error("Wrong table name used");
            }


            if (isTableName(right))
            {
                if (divideAndGetTable(right) ==  table1Name)
                {
                    right = row1.HGET(divideAndGetColumn(left));
                }
                else if (divideAndGetTable(right) ==  table2Name)
                {
                    right = row2.HGET(divideAndGetColumn(right));
                }
                else
                {
                    throw runtime_error("Wrong table name used");
                }
            }

            results.addtail(left == right); 
        }
        else
        {
            throw runtime_error("Wrong syntax in chosen columns");
        }
    }

    return getFinalResult(results, operators);

}

void handleSELECT(LinkedList<string> inputList, int clientSocket)
 {
    lock_guard<mutex> selectionMUTEX(selectMutex);
    LinkedList<string> selectedColumns = getSelectedTablesSELECT(inputList);
    LinkedList<string> selectedTables = getSelectedTablesFROM(inputList);

    LinkedList<string> conditions;
    LinkedList<string> operators;
    
    bool startWrite = 0;
    string element;
    for (int i = 0; i < inputList.size(); i++)
    {
        element = inputList.get(i);
        if (startWrite)
        {
            if (element == "OR" || element == "AND")
            {
                operators.addtail(element);
            }
            else
            {
                conditions.addtail(element);
            }
        }
        if (element == "WHERE") startWrite = 1;
        
    }

    if (selectedTables.size() == 2 && selectedColumns.size() == 2)
    {

        LinkedList<HASHtable<string>> table1 = readTable(selectedTables.get(0));
        LinkedList<HASHtable<string>> table2 = readTable(selectedTables.get(1));

        LinkedList<string> table1ColNames = getColumnNamesFromTable(selectedTables.get(0));
        LinkedList<string> table2ColNames = getColumnNamesFromTable(selectedTables.get(1));

        for (int i = 1; i < table1.size(); i++)
        {
            HASHtable<string> currentRowFirst = table1.get(i);
            for (int j = 1; j < table2.size(); j++)
            {
                HASHtable<string> currentRowSecond = table2.get(j);
                if (checkCondition(selectedTables.get(0),currentRowFirst, selectedTables.get(1), currentRowSecond, conditions, operators))
                {
                    string outputForClient = table1.get(i).HGET(divideAndGetColumn(selectedColumns.get(0))) + " " 
                    + table2.get(j).HGET(divideAndGetColumn(selectedColumns.get(1))) + '\n';
                    send(clientSocket, outputForClient.c_str(), outputForClient.size(), 0);
                }

            }

        }
    }
    else
    {
        throw runtime_error("Wrong amount of tables chosen!");
    }
}

bool checkCondition(string table1Name, HASHtable<string> row1, 
                    LinkedList<string> conditions, LinkedList<string> operators)
{

    LinkedList<bool> results;


    for (int i = 0; i < conditions.size(); i += 3)
    {
        string left = conditions.get(i);
        string op = conditions.get(i + 1);
        string right = conditions.get(i + 2);

        if (isTableName(left) && op == "=" && !isTableName(right))
        {
            if (divideAndGetTable(left) ==  table1Name)
            {
                left = row1.HGET(divideAndGetColumn(left));
            }
            else
            {
                throw runtime_error("Wrong condition for delete");
            }

            results.addtail(left == right); 
        }
        else
        {
            throw runtime_error("Wrong syntax in delete");
        }
    }

    return getFinalResult(results, operators);

}

void handleDELETE(LinkedList<string> inputList)
{
    lock_guard<mutex> deletionMUTEX(delMutex);
    LinkedList<string> selectedTables = getSelectedTablesFROM(inputList);
    string tableName = selectedTables.get(0);
    LinkedList<HASHtable<string>> table = readTable(tableName);

    lockTable(tableName);

    LinkedList<string> conditions;
    LinkedList<string> operators;
    
    bool startWrite = 0;
    string element;
    for (int i = 0; i < inputList.size(); i++)
    {
        element = inputList.get(i);
        if (startWrite)
        {
            if (element == "OR" || element == "AND")
            {
                operators.addtail(element);
            }
            else
            {
                conditions.addtail(element);
            }
        }
        if (element == "WHERE") startWrite = 1;
        
    }

    LinkedList<HASHtable<string>> newTable;

    if (selectedTables.size() == 1)
    {
        for(int i = 0; i < table.size(); i++)
        {
            HASHtable<string> currentRow = table.get(i);
            if (!checkCondition(tableName, currentRow, conditions, operators))
            {
                newTable.addtail(currentRow);
            }
            else decreasePKSEQ(tableName);
        }

        uploadTable(newTable, tableName);
    } 
    else
    {
        throw runtime_error("Wrong syntax in delete from table");
    }

    unlockTable(tableName);
}

void MENU(auto clientInput, auto clientSocket)
{
    try
    {   
        LinkedList<string> inputList = parseCommand(clientInput);
        inputList.print();
        string operation = inputList.get(0);
        
        if (operation == "exit" || operation == "EXIT")
        {
            cout << "Waiting for next query" << endl;
            return;
        }

        if (operation == "SELECT")
        {
            handleSELECT(inputList, clientSocket);
        }
        else if (operation == "DELETE")
        {
            handleDELETE(inputList);
        }
        else if (operation == "INSERT")
        {
            handleINPUT(inputList);
        }
        else
        {
            throw runtime_error("Wrong operation called!");
        }    
    }
    catch (const runtime_error& e)
    {
        lock_guard<mutex> lock(menu_mtx);
        cerr << "ERROR: " << e.what() << endl;
        string errorMessage = "ERROR: " + string(e.what());
        send(clientSocket, errorMessage.c_str(), errorMessage.size(), 0);

    }
}

sockaddr_in defineServer(const string& ipAddress, const int& port)
{
    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);
    if (ipAddress == "localhost")
    {
        inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr);

    }
    else
    {
        serverAddress.sin_addr.s_addr = INADDR_ANY;
    }

    cout << "Server started with values: " << endl <<
            "IP: " << ipAddress << " and Port: " << port << endl;

    return serverAddress;
}

void handleClient(int clientSocket)
{
    char buffer[1024];
    
    int bytesReceived = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytesReceived > 0) {
        buffer[bytesReceived] = '\0';
        {
            lock_guard<mutex> lock(mtx);
            cout << "Received message: " << buffer << endl;
        }


        try
        {
            MENU(buffer, clientSocket);
        }
        catch (const exception& e)
        {
            cerr << "Error in client handling: " << e.what() << endl;
        }
        
    }

    {
        lock_guard<mutex> lock(mtx);
        cout << "Client disconnected." << endl;
    }
    close(clientSocket); // Закрываем клиентский сокет после обработки одного запроса
}

void serverListener(int serverSocket)
{
    sockaddr_in clientAddress;
    socklen_t clientAddressSize = sizeof(clientAddress);
    while (true)
    {
        int clientSocket = accept(serverSocket, (sockaddr*)&clientAddress, &clientAddressSize);
        if (clientSocket == -1)
        {
            cerr << "Error occurred while connecting to client!" << endl;
            continue;
        }

        {
            lock_guard<mutex> lg(mtx);
            cout << "Client connected!" << endl;
        }

        // Создаем поток для каждого клиента, чтобы обеспечить многопоточность
        thread clientThread(handleClient, clientSocket);
        clientThread.detach(); // Отсоединяем поток для независимой обработки клиента
    }
}

void createCurrencyPairs(LinkedList<string>& curruncy)
{
    int counterId = 1;
    for (int i = 0; i < curruncy.size(); i++)
    {
        string baseCurr = curruncy.get(i);
        for (int j = 0; j < curruncy.size(); j++)
        {
            if (i == j) continue;
            LinkedList<string> insertValues;
            insertValues.addtail(to_string(counterId));
            insertValues.addtail(to_string(i+1));
            insertValues.addtail(to_string(j+1));
            insert(insertValues, "pair");
            counterId++;
        }
    }
    

}

void useConfigFile(string& ip, int& port, bool isCreated)
{
    
    LinkedList<string> tablePaths;
    string path = filesystem::current_path();

    string jsonContent = readJSON(path.substr(0,21) + "/config.json"); //Reading json
    cJSON* json = cJSON_Parse(jsonContent.c_str()); //Parsing .json file

    cJSON* ipAddress = cJSON_GetObjectItem(json, "database_ip"); //Parsing ip address from config
    ip = ipAddress->valuestring;

    cJSON* portAddress = cJSON_GetObjectItem(json, "database_port"); //Parsing ip address from config
    port = portAddress->valueint;
    
    if (isCreated == false)
    {
        cJSON* structure = cJSON_GetObjectItem(json,"structure"); //Parsing structure

        LinkedList<string> currencyList;
        int idCounter = 1;
        for (cJSON* currency = cJSON_GetObjectItem(json,"lots"); currency->child != nullptr; currency->child = currency->child->next)
        {
            LinkedList<string> valuesForInsert;
            
            currencyList.addtail(currency->child->valuestring);

            valuesForInsert.addtail(to_string(idCounter));
            valuesForInsert.addtail(currency->child->valuestring);
            insert(valuesForInsert, "lot");
            idCounter++;
        }
  
        createCurrencyPairs(currencyList);
    }

    

    cJSON_Delete(json);
}

string generateUserToken()
{
    boost::uuids::random_generator generator;
    boost::uuids::uuid newUUID = generator();
    return to_string(newUUID);
}

bool checkTokenPrescence(auto iter, auto req)
{
    if (iter == req.end())
    {
        return false;
    }
    return true;
}

string getUserIDByToken(string token)
{
    LinkedList<HASHtable<string>> tableLines = getTableLines("user");
    LinkedList<string> columnsInTable = getColumnNamesFromTable("user");
    for (int i = 1; i <= tableLines.size(); i++)
    {
        if (tableLines.get(i).HGET(columnsInTable.get(2)) == token)
        {
            return tableLines.get(i).HGET(columnsInTable.get(0));
        }
        
    }


        //CHECK THIS FUNCTION ON WORKING DATA


    return "not_found";

}

float stringToFloat(const string& input) 
{
    if (input.empty()) 
    {
        cout << "Wrong input: empty string is not a valid number.\n";
        exit(-1);
    }

    bool after_tochki = false;
    int point_count = 0;

    for (char ch : input)
    {
        if (!isdigit(ch)) 
        {
            if (ch == '.') 
            {
                point_count++;
                if (point_count > 1) 
                {
                    cout << "Wrong input: more than one decimal point!\n";
                    exit(-1);
                }
                after_tochki = true;
                continue;
            }
            cout << "Wrong input: " << ch << " isn't a number!\n";
            exit(-1);
        }
    }

    size_t pos = input.find('.');
    string before_str = (pos != string::npos) ? input.substr(0, pos) : input; // Часть до точки
    string after_str = (pos != string::npos) ? input.substr(pos + 1) : "";    // Часть после точки

    float before_num = 0;
    float after_num = 0;

    // Сборка целой части
    for (int i = 0; i < before_str.size(); ++i) 
    {
        before_num += (before_str[i] - '0') * pow(10, before_str.size() - i - 1);
    }

    // Сборка дробной части
    for (int i = 0; i < after_str.size(); ++i) 
    {
        after_num += (after_str[i] - '0') * pow(10, -(i + 1));
    }

    return before_num + after_num;
}

bool checkOrderRequest(json::value& request)
{
	bool isCorrect = true;
	isCorrect = isCorrect && request.as_object().contains("pair_id");
	isCorrect = isCorrect && request.as_object().contains("quantity");
	isCorrect = isCorrect && request.as_object().contains("price");
	isCorrect = isCorrect && request.as_object().contains("type");
	
    string type;
    type = request.as_object()["type"].as_string();
    if (!(type == "sell" || type == "buy")) isCorrect = false;

	return isCorrect;
}

void updateActiveOrders(LinkedList<HASHtable<string>>& orders, LinkedList<string>& activePairs, LinkedList<string> colNames)
{
    int oldLines = orders.size();

    for (int i = 0; i < oldLines; i++)
    {
        string currentPair = orders.get(i).HGET(colNames.get(2));
        if (activePairs.search(currentPair))
        {
            orders.addtail(orders.get(i));
        }
    }
    for (int i = 0; i < oldLines; i++)
    {
        orders.delhead();
    }

}

string getLeftLotFromPair(string& pairID)
{
    LinkedList<HASHtable<string>> pairTable = getTableLines("pair");
    LinkedList<string> columns = getColumnNamesFromTable("pair");
    string leftLot = "";

    for (int i = 1; i < pairTable.size(); i++)
    {
        string currentPairID = pairTable.get(i).HGET(columns.get(0));
        if (pairID == currentPairID)
        {
            leftLot = pairTable.get(i).HGET(columns.get(1));
            return leftLot;
        }
    }
    return "not found";
}

string getRightLotFromPair(string& pairID)
{
    LinkedList<HASHtable<string>> pairTable = getTableLines("pair");
    LinkedList<string> columns = getColumnNamesFromTable("pair");
    string rightLot = "";

    for (int i = 1; i < pairTable.size(); i++)
    {
        string currentPairID = pairTable.get(i).HGET(columns.get(0));
        if (pairID == currentPairID)
        {
            rightLot = pairTable.get(i).HGET(columns.get(2));
            return rightLot;
        }
    }
    return "not found";
}

void updateBalance(string& newBalance, string& userID,string& lotID)
{
    LinkedList<string> columns = getColumnNamesFromTable("user_lot");
    LinkedList<HASHtable<string>> userLotTable = getTableLines("user_lot");
    LinkedList<HASHtable<string>> newTable;
    newTable.addtail(userLotTable.get(0));
    bool isChanged = false;

    for (int i = 1; i < userLotTable.size(); i++)
    {
        string userIDtable = userLotTable.get(i).HGET(columns.get(0));
        string lotTable = userLotTable.get(i).HGET(columns.get(1));
        if (userIDtable == userID && lotID == lotTable)
        {
            userLotTable.get(i).HSET(columns.get(2), newBalance);
            isChanged = true;
        }
        newTable.addtail(userLotTable.get(i));
    }

    if (isChanged) 
    {
        uploadTable(newTable, "user_lot");
    }
    
}

string getUserBalanceByLotID(string& userID, string& lotID)
{
    LinkedList<string> columns = getColumnNamesFromTable("user_lot");
    LinkedList<HASHtable<string>> userLotTable = getTableLines("user_lot");
    string balance = "not found";

    for (int i = 1; i < userLotTable.size(); i++)
    {
        string userInTable = userLotTable.get(i).HGET(columns.get(0));
        string lotInTable = userLotTable.get(i).HGET(columns.get(1));
        
        if (userInTable == userID && lotInTable == lotID)
        {
            balance = userLotTable.get(i).HGET(columns.get(2));
            break;
        }
    }

    return balance;
} 

void matchOrders(LinkedList<HASHtable<string>>& sellOrders, LinkedList<HASHtable<string>>& buyOrders, LinkedList<string> colNames) {
    for (size_t i = 0; i < sellOrders.size(); i++) {
        HASHtable<string> currentSellOrder = sellOrders.get(i);
        string sellerCurrPairID = currentSellOrder.HGET(colNames.get(2));
        float sellerPrice = stof(currentSellOrder.HGET(colNames.get(4)));
        string statusS = currentSellOrder.HGET(colNames.get(6));

        if (statusS == "close") {
            continue;
        }

        for (size_t j = 0; j < buyOrders.size(); j++) {
            HASHtable<string> currentBuyOrder = buyOrders.get(j);
            string buyerCurrPairID = currentBuyOrder.HGET(colNames.get(2));
            string statusB = currentBuyOrder.HGET(colNames.get(6));
            float buyerPrice = stof(currentBuyOrder.HGET(colNames.get(4)));

            string sellerID = currentSellOrder.HGET(colNames.get(1));
            string buyerID = currentBuyOrder.HGET(colNames.get(1));

            if (sellerCurrPairID == buyerCurrPairID && sellerPrice <= buyerPrice && statusB != "close" && sellerID != buyerID) {
                float buyAmount = stof(currentBuyOrder.HGET(colNames.get(3)));
                float sellAmount = stof(currentSellOrder.HGET(colNames.get(3)));

                float transactionAmount = min(sellAmount, buyAmount);

                buyAmount -= transactionAmount;
                sellAmount -= transactionAmount;

                string buyLotID = getRightLotFromPair(buyerCurrPairID);
                string sellLotID = getLeftLotFromPair(buyerCurrPairID);

                string currentPrice = currentSellOrder.HGET(colNames.get(4));

                float sellerGet = transactionAmount * stof(currentPrice);
                float buyerGet = transactionAmount;

                string sellerBalance = to_string(stof(getUserBalanceByLotID(sellerID, sellLotID)) + sellerGet);
                string buyerBalance = to_string(stof(getUserBalanceByLotID(buyerID, buyLotID)) - transactionAmount);

                updateBalance(buyerBalance, buyerID, buyLotID);
                updateBalance(sellerBalance, sellerID, sellLotID);

                if (sellAmount == 0) {
                    currentSellOrder.HSET(colNames.get(6), "close");
                } else {
                    currentSellOrder.HSET(colNames.get(3), to_string(sellAmount));
                }

                if (buyAmount == 0) {
                    currentBuyOrder.HSET(colNames.get(6), "close");
                } else {
                    currentBuyOrder.HSET(colNames.get(3), to_string(buyAmount));
                }

                sellOrders.set(i, currentSellOrder);
                buyOrders.set(j, currentBuyOrder);

                if (sellAmount == 0) {
                    break;
                }
            }
        }
    }
}

void trade() 
{
    LinkedList<HASHtable<string>> buyOrders;
    LinkedList<HASHtable<string>> sellOrders;

    LinkedList<string> sellingPairs;
    LinkedList<string> buyingPairs;
    LinkedList<string> activePairs;

    LinkedList<HASHtable<string>> rawOrders = getTableLines("order");
    LinkedList<string> columns = getColumnNamesFromTable("order");

    for (int i = 1; i < rawOrders.size(); i++) 
    {
        string status = rawOrders.get(i).HGET(columns.get(6));
        if (status == "open") 
        {
            string type = rawOrders.get(i).HGET(columns.get(5));
            string pairID = rawOrders.get(i).HGET(columns.get(2));

            if (type == "buy") 
            {
                if (!buyingPairs.search(pairID)) 
                {
                    buyingPairs.addtail(pairID);
                }
                buyOrders.addtail(rawOrders.get(i));
            } 
            else if (type == "sell") 
            {
                if (!sellingPairs.search(pairID)) 
                {
                    sellingPairs.addtail(pairID);
                }
                sellOrders.addtail(rawOrders.get(i));
            }
        }
    }

    for (int i = 0; i < buyingPairs.size(); i++) 
    {
        if (sellingPairs.search(buyingPairs.get(i))) 
        {
            activePairs.addtail(buyingPairs.get(i));
        }
    }

    if (activePairs.size() == 0) 
    {
        return;
    }

    updateActiveOrders(buyOrders, activePairs, columns);
    updateActiveOrders(sellOrders, activePairs, columns);

    matchOrders(sellOrders, buyOrders, columns);

    LinkedList<HASHtable<string>> newOrderTable;
    newOrderTable.addtail(rawOrders.get(0));

    for (int i = 1; i < rawOrders.size(); i++) 
    {
        string orderID = rawOrders.get(i).HGET(columns.get(0));
        bool added = false;

        for (int j = 0; j < buyOrders.size(); j++) 
        {
            if (buyOrders.get(j).HGET(columns.get(0)) == orderID) 
            {
                newOrderTable.addtail(buyOrders.get(j));
                added = true;
                break;
            }
        }

        if (!added) 
        {
            for (int j = 0; j < sellOrders.size(); j++) 
            {
                if (sellOrders.get(j).HGET(columns.get(0)) == orderID) 
                {
                    newOrderTable.addtail(sellOrders.get(j));
                    added = true;
                    break;
                }
            }
        }

        if (!added) 
        {
            newOrderTable.addtail(rawOrders.get(i));
        }
    }

    uploadTable(newOrderTable, "order");
}

bool canCreateOrder(string& userID, string& lotID, double newOrderAmount, double newOrderPrice, string newOrderType) 
{
    double reservedAmount = 0.0;

    // Получаем ID расчетной валюты из lotID
    string calculationCurrency = lotID;

    // Получаем таблицу заявок
    LinkedList<HASHtable<string>> orderTable = getTableLines("order");
    LinkedList<string> columns = getColumnNamesFromTable("order");

    // Перебираем строки таблицы
    for (int i = 1; i < orderTable.size(); i++) 
    {
        string userInTable = orderTable.get(i).HGET(columns.get(1)); // ID пользователя
        string orderType = orderTable.get(i).HGET(columns.get(5));  // Тип заявки (buy/sell)
        string status = orderTable.get(i).HGET(columns.get(6));     // Статус заявки (open/closed)
        string pair = orderTable.get(i).HGET(columns.get(2));       // Валютная пара
        double amount = stod(orderTable.get(i).HGET(columns.get(3))); // Объем
        double price = stod(orderTable.get(i).HGET(columns.get(4))); // Цена

        // Пропускаем заявки других пользователей или с закрытым статусом
        if (userInTable != userID || status != "open") 
        {
            continue;
        }

        string leftCurrency = getLeftLotFromPair(pair);   // Левая валюта
        string rightCurrency = getRightLotFromPair(pair); // Правая валюта

        // Проверяем, содержится ли расчетная валюта в паре
        bool isLeft = (calculationCurrency == leftCurrency);
        bool isRight = (calculationCurrency == rightCurrency);

        if (!isLeft && !isRight) 
        {
            continue; // Если расчетная валюта не в паре, пропускаем
        }

        // Подсчитываем резерв
        if (orderType == "sell") 
        {
            if (isLeft) 
            {
                reservedAmount += amount; // Резервируем объем
            } 
            else if (isRight) 
            {
                reservedAmount += amount * price; // Резервируем стоимость
            }
        } 
        else if (orderType == "buy") 
        {
            if (isLeft) 
            {
                reservedAmount += amount * price; // Резервируем стоимость
            } 
            else if (isRight) 
            {
                reservedAmount += amount; // Резервируем объем
            }
        }
    }

    // Учитываем новую заявку
    string leftCurrency = getLeftLotFromPair(lotID);
    string rightCurrency = getRightLotFromPair(lotID);

    if (newOrderType == "sell") 
    {
        if (calculationCurrency == leftCurrency) 
        {
            reservedAmount += newOrderAmount; // Резервируем объем
        } 
        else if (calculationCurrency == rightCurrency) 
        {
            reservedAmount += newOrderAmount * newOrderPrice; // Резервируем стоимость
        }
    } 
    else if (newOrderType == "buy") 
    {
        if (calculationCurrency == leftCurrency) 
        {
            reservedAmount += newOrderAmount * newOrderPrice; // Резервируем стоимость
        } 
        else if (calculationCurrency == rightCurrency) 
        {
            reservedAmount += newOrderAmount; // Резервируем объем
        }
    }

    // Получаем баланс пользователя по расчетной валюте
    double userBalance = stod(getUserBalanceByLotID(userID, calculationCurrency));

    // Проверяем, хватает ли баланса
    return userBalance >= reservedAmount;
}


void handle_request(const http::request<http::string_body>& req, http::response<http::string_body>& res)
{
    try
    {
        string targetPath = string(req.target());
        if (req.method() == http::verb::post)
        {
            if (targetPath == "/user")
            {
                json::value request = json::parse(req.body());

                string userToken = generateUserToken();
                string username;

                if (request.as_object().contains("username"))
                {
                    username = request.as_object()["username"].as_string();
                }
                else
                {
                    res.result(http::status::bad_request);
                    res.set(http::field::content_type, "text/plain");
                    res.body() = R"({"error":"Missing username"})";
                    res.prepare_payload();
                    return;
                }

                LinkedList<string> userInfo;
                int id = getPKSEQ("user");
                userInfo.addtail(to_string(id));
                userInfo.addtail(username);
                userInfo.addtail(userToken);
                insert(userInfo, "user");


                json::value response =
                {
                    {"key:", userToken}
                };

                res.result(http::status::ok);
                res.set(http::field::content_type, "application/json");
                res.body() = json::serialize(response);
                res.prepare_payload();

                int lots = getPKSEQ("lot");
                for (int i = 1; i < lots; i++)
                {
                    LinkedList<string> dataForInsert;

                    dataForInsert.addtail(to_string(id));
                    dataForInsert.addtail(to_string(i));
                    dataForInsert.addtail("1000");
                    insert(dataForInsert, "user_lot");
                }

                return;
            }
            else if (targetPath == "/order")
            {

                auto userToken = req.find("X-USER-KEY");
                if (!checkTokenPrescence(userToken, req))
                {
                    res.result(http::status::bad_request);
                    res.set(http::field::content_type, "application/json");
                    res.body() = R"({"error":"missing X-USER-KEY"})";
                    res.prepare_payload();
                    return;
                }
                string userKey = userToken->value();
                string userID = getUserIDByToken(userKey);


                json::value request = json::parse(req.body());
                string pairID, quantity, price, type;
                LinkedList<string> dataForInsert;
                int orderID = getPKSEQ("order");

                if (checkOrderRequest(request))
                {
                    pairID = to_string(request.as_object()["pair_id"].as_int64());
                    quantity = to_string(request.as_object()["quantity"].as_double());
                    price = to_string(request.as_object()["price"].as_double());
                    type = request.as_object()["type"].as_string();

                    dataForInsert.addtail(to_string(orderID));
                    dataForInsert.addtail(userID);
                    dataForInsert.addtail(pairID);
                    dataForInsert.addtail(quantity);
                    dataForInsert.addtail(price);
                    dataForInsert.addtail(type);
                    dataForInsert.addtail("open");

                    string lotID;
                    if (type == "buy")
                    {
                        lotID = getLeftLotFromPair(pairID);
                        
                    }
                    else if (type == "sell")
                    {
                        lotID = getRightLotFromPair(pairID);
                    }
                    
                    if (canCreateOrder(userID, lotID, stod(quantity), stod(price), type))
                    {
                        trade();
                        insert(dataForInsert, "order");
                    }
                    else
                    {
                        res.result(http::status::bad_request);
                        res.set(http::field::content_type, "text/plain");
                        res.body() = R"({"error":"Not enough balance to make order"})";
                        res.prepare_payload();
                        return;
                    }

                    
                }
                else
                {
                    res.result(http::status::bad_request);
                    res.set(http::field::content_type, "text/plain");
                    res.body() = R"({"error":"Missing some order details"})";
                    res.prepare_payload();
                    return;
                }


                trade();

                json::value response =
                {
                    {"order_id:", orderID}
                };

                res.result(http::status::ok);
                res.set(http::field::content_type, "application/json");
                res.body() = json::serialize(response);
                res.prepare_payload();
                return;
            }
        }
        else if (req.method() == http::verb::get)
        {
            auto userToken = req.find("X-USER-KEY");
            if (!checkTokenPrescence(userToken, req))
            {
                res.result(http::status::bad_request);
                res.set(http::field::content_type, "application/json");
                res.body() = R"({"error":"missing X-USER-KEY"})";
                res.prepare_payload();
                return;
            }

            string userKey = userToken->value();
            string userID = getUserIDByToken(userKey);

            if (userID == "not_found")
            {
                res.result(http::status::bad_request);
                res.set(http::field::content_type, "application/json");
                res.body() = R"({"error":"user not found!"})";
                res.prepare_payload();
                return;
            }

            if (targetPath == "/order")
            {
                int orders = getPKSEQ("order");
                json::array response;
                LinkedList<HASHtable<string>> orderTable = getTableLines("order");
                LinkedList<string> columns = getColumnNamesFromTable("order");


                string userID = getUserIDByToken(userKey);
                if (userID == "not_found")
                {
                    res.result(http::status::bad_request);
                    res.set(http::field::content_type, "application/json");
                    res.body() = R"({"error":"user not found!"})";
                    res.prepare_payload();
                    return;
                }


                bool dataFound = false;
                for (int i = 1; i < orders; i++)
                {
                    if (userID == orderTable.get(i).HGET(columns.get(1)))
                    {
                        json::value item =
                        {
                            {"order_id", orderTable.get(i).HGET(columns.get(0))},
                            {"user_id", userID},
                            {"lot_id", stoi(orderTable.get(i).HGET(columns.get(2)))},
                            {"quantity", stof(orderTable.get(i).HGET(columns.get(3)))},
                            {"type", orderTable.get(i).HGET(columns.get(5))},
                            {"price", stof(orderTable.get(i).HGET(columns.get(4)))},
                            {"closed", orderTable.get(i).HGET(columns.get(6))}
                        };
                        response.push_back(item);
                        dataFound = true;
                    }
                }

                if (dataFound)
                {
                    res.result(http::status::ok);
                    res.set(http::field::content_type, "application/json");
                    res.body() = json::serialize(response);
                    res.prepare_payload();
                    return;
                }
                else
                {
                    res.result(http::status::not_found); // Можно вернуть статус 404
                    res.set(http::field::content_type, "application/json");
                    res.body() = R"({"error":"no orders found for the user"})";
                    res.prepare_payload();
                }
            }
            else if (targetPath == "/lot")
            {
                

                LinkedList<HASHtable<string>> lotTable = getTableLines("lot");
                LinkedList<string> columns = getColumnNamesFromTable("lot");
                int linesInTable = getPKSEQ("lot");
                
                json::array response;
                for (int i = 1; i < linesInTable; i++)
                {
                    int id = stoi(lotTable.get(i).HGET(columns.get(0)));
                    string name = lotTable.get(i).HGET(columns.get(1));

                    json::value item =
                    {
                        {"lot_id", id},
                        {"name", name}
                    };
                    response.push_back(item);
                }

                res.result(http::status::ok);
                res.set(http::field::content_type, "application/json");
                res.body() = json::serialize(response);
                res.prepare_payload();
                return;
            }
            else if (targetPath == "/pair")
            {
                json::array response;
                LinkedList<string> columns = getColumnNamesFromTable("pair");
                LinkedList<HASHtable<string>> tableLine = getTableLines("pair");

                int linesInTable = getPKSEQ("pair");

                for (int i = 1; i < linesInTable; i++)
                {
                    json::value item =
                    {
                        {"pair_id", stoi(tableLine.get(i).HGET(columns.get(0)))},
                        {"sale_lot_id", stoi(tableLine.get(i).HGET(columns.get(1)))},
                        {"buy_lot_id", stoi(tableLine.get(i).HGET(columns.get(2)))}
                    };
                    response.push_back(item);
                }

                res.result(http::status::ok);
                res.set(http::field::content_type, "application/json");
                res.body() = json::serialize(response);
                res.prepare_payload();
                return;
            }
            else if (targetPath == "/balance")
            {
                int lines = getPKSEQ("user_lot");
                json::array response;
                LinkedList<string> columns = getColumnNamesFromTable("user_lot");
                LinkedList<HASHtable<string>> tableLine = getTableLines("user_lot");

                
                for (int i = 1; i < lines; i++)
                {
                    if (userID == tableLine.get(i).HGET(columns.get(0)))
                    {
                        json::value item =
                        {
                            {"lot_id", stoi(tableLine.get(i).HGET(columns.get(1)))},
                            {"quantity", stof(tableLine.get(i).HGET(columns.get(2)))}
                        };
                        response.push_back(item);
                    }
                    
                }

                res.result(http::status::ok);
                res.set(http::field::content_type, "application/json");
                res.body() = json::serialize(response);
                res.prepare_payload();
                return;
            }
        }
        else if (req.method() == http::verb::delete_)
        {

            auto userToken = req.find("X-USER-KEY");
                if (!checkTokenPrescence(userToken, req))
                {
                    res.result(http::status::bad_request);
                    res.set(http::field::content_type, "application/json");
                    res.body() = R"({"error":"missing X-USER-KEY"})";
                    res.prepare_payload();
                    return;
                }
                string userKey = userToken->value();

                string userID = getUserIDByToken(userKey);
                if (userID == "not_found")
                {
                    res.result(http::status::bad_request);
                    res.set(http::field::content_type, "application/json");
                    res.body() = R"({"error":"user not found!"})";
                    res.prepare_payload();
                    return;
                }


            if (targetPath == "/order")
            {

                json::value request = json::parse(req.body());


                string orderID;

                if (request.as_object().contains("order_id"))
                {
                    orderID = to_string(request.as_object()["order_id"].as_int64());
                }
                else
                {
                    res.result(http::status::bad_request);
                    res.set(http::field::content_type, "text/plain");
                    res.body() = R"({"error":"Missing order_id"})";
                    res.prepare_payload();
                    return;
                }

                LinkedList<HASHtable<string>> oldTable = readTable("order");
                LinkedList<string> columns = getColumnNamesFromTable("order");
                
                bool isChaged = false;
                for (int i = 1; i < oldTable.size(); i++)
                {
                    if (orderID == oldTable.get(i).HGET(columns.get(0)) && 
                        userID == oldTable.get(i).HGET(columns.get(1))  &&
                        "open" == oldTable.get(i).HGET(columns.get(6)))
                    {
                        oldTable.get(i).HSET(columns.get(6),"close");
                        isChaged = true;
                        break;
                    }       
                }

                uploadTable(oldTable, "order");

                if (!isChaged)
                {
                    res.result(http::status::not_found);
                    res.set(http::field::content_type, "text/plain");
                    res.body() = R"({"error":"order not found or belong to smbd else!"})";
                    res.prepare_payload();
                    return;
                }
                else
                {
                    res.result(http::status::not_found);
                    res.set(http::field::content_type, "text/plain");
                    res.body() = R"({"response":"order has been successfully deleted"})";
                    res.prepare_payload();
                    return;
                }
            }
        }
        else
        {
            json::value response =
            {
                {"error", "Unsupported HTTP method"}
            };
            res.result(http::status::bad_request);
            res.set(http::field::content_type, "application/json");
            res.body() = json::serialize(response);
            res.prepare_payload();
        }
    }
    catch (const std::exception& e)
    {
        json::value response_data =
        {
            {"error", e.what()}
        };

        res.result(http::status::internal_server_error);
        res.set(http::field::content_type, "application/json");
        res.body() = json::serialize(response_data);
        res.prepare_payload();
    }
}

void do_session(std::shared_ptr<tcp::socket> socket) 
{
    try 
    {
        beast::flat_buffer buffer;

        // Чтение HTTP-запроса
        http::request<http::string_body> req;
        http::read(*socket, buffer, req);

        // Формирование HTTP-ответа
        http::response<http::string_body> res;
        handle_request(req, res);

        // Отправка HTTP-ответа
        http::write(*socket, res);
    } 
    catch (const std::exception& e) 
    {
        cerr << "Session error: " << e.what() << "\n";
    }
}

void do_accept(tcp::acceptor& acceptor, net::io_context& ioc) 
{
    acceptor.async_accept([&acceptor, &ioc](boost::system::error_code ec, tcp::socket socket) 
    {
        if (!ec) 
        {
            auto shared_socket = make_shared<tcp::socket>(move(socket));
            do_session(shared_socket);
        } 
        else 
        {
            cerr << "Accept error: " << ec.message() << "\n";
        }

        // Повторное ожидание соединения
        do_accept(acceptor, ioc);
    });
}

void handleAPI()
{
    try 
    {
        net::io_context ioc;

        // Создаем acceptor для прослушивания порта 8080
        tcp::acceptor acceptor(ioc, {tcp::v4(), 8080});

        cout << "Server is running on port 8080...\n";

        do_accept(acceptor, ioc);

        ioc.run();
    } 
    catch (const exception& e) 
    {
        cerr << "Error: " << e.what() << "\n";
    }

}

void serverHandling(const string& IP,const int& PORT)
{
    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1)
    {
        cout << "Server socket creation failed!";
        exit(-1);
    }

    sockaddr_in serverAddress = defineServer(IP, PORT);
    if (bind(serverSocket, (sockaddr*)&serverAddress, sizeof(serverAddress)) == -1)
    {
        cout << "Error binding socket";
        exit(-1);
    }

    listen(serverSocket, 5);
    cout << "WORKS" << endl;

    //jthread Api();
    handleAPI();
    //serverListener(serverSocket); // Запускаем обработчик входящих подключений
    //jthread mainServer(serverListener, ref(serverSocket));
    
    
    close(serverSocket);
}

void createDataBase(string& IP, int& PORT)
{   
 
    LinkedList<string> tablePaths;
    string jsonContent = readJSON("schema.json"); //Reading json
    cJSON* json = cJSON_Parse(jsonContent.c_str()); //Parsing .json file
    cJSON* schemaName = cJSON_GetObjectItem(json, "name"); //Parsing DataBase name
    string DataBaseName = schemaName->valuestring;

    if (jsonContent.empty())
    {
        throw runtime_error("Error reading schema, content is empty!");
    }

    fstream checkDB("DataBaseFlag");
    if (checkDB.is_open())
    {
        string path = filesystem::current_path();
        filesystem::current_path(path + "/" + DataBaseName);
        useConfigFile(IP, PORT, 1);
        checkDB.close();
        return;
    }

    
    ofstream DataBaseFlag("DataBaseFlag"); //Creating presence database flag
    
    DataBaseFlag.close();

    

    
    if (json == nullptr)
    {
        throw runtime_error("Error parsing schema file!");
    }

    cJSON* schemaLimit = cJSON_GetObjectItem(json, "tuples_limit"); //Parsing tuple limit
    int tuplesLimit = schemaLimit->valueint;

    
    
    createDir(DataBaseName); //Creating DataBase folder
    

    string path = filesystem::current_path();

    filesystem::current_path(path + "/" + DataBaseName);
    

    

    cJSON* structure = cJSON_GetObjectItem(json,"structure"); //Parsing structure

    for (cJSON* table = structure->child; table != nullptr; table = table->next) //Going through tables
    {
        string subFolderPath = filesystem::current_path();
        string tableName = table->string;
        subFolderPath =  "/" + tableName;
        tablePaths.addhead(subFolderPath);

        createDir(subFolderPath);

        createFilesInSubFolder(table, structure, subFolderPath);
    }

    useConfigFile(IP, PORT, 0);
    
    cJSON_Delete(json);
}


int main()
{
    setlocale(LC_ALL, "RU");
    string IP;
    int PORT;
    createDataBase(IP, PORT);

    serverHandling(IP, PORT);


    return 0;
}
