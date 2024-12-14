#include <iostream>
#include <string>
#include <stdexcept>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

const int SERVER_PORT = 7432;
const string SERVER_IP = "127.0.0.1"; // IP сервера, который нужно использовать для подключения

// Функция для подключения к серверу, отправки запроса и получения ответа
void connectToServerAndSendRequest(const string& request) 
{
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (clientSocket == -1) 
    {
        cerr << "Error creating client socket!" << endl;
        return;
    }

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP.c_str(), &serverAddress.sin_addr);

    if (connect(clientSocket, (sockaddr*)&serverAddress, sizeof(serverAddress)) == -1) 
    {
        cerr << "Error connecting to server!" << endl;
        close(clientSocket);
        return;
    }

    // Отправляем запрос на сервер
    send(clientSocket, request.c_str(), request.size(), 0);

    // Получаем ответ от сервера
    char buffer[1024];
    int bytesReceived = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
    if (bytesReceived > 0) 
    {
        buffer[bytesReceived] = '\0';
        cout << "Server response: " << endl << buffer;
    } 
    else 
    {
        cout << "No response from server." << endl;
    }

    close(clientSocket); // Закрываем клиентский сокет после отправки запроса
}

// Функция для отображения меню
void displayMenu() 
{
    cout << "\n==== Client Menu ====" << endl;
    cout << "1. Use - Connect to server and send a query" << endl;
    cout << "2. Exit - Close the client" << endl;
    cout << "=====================" << endl;
}

void clientMenu() 
{

    while (true) 
    {
        displayMenu();
        
        cout << "Select an option: ";
        string choice;
        getline(cin, choice);

        if (choice == "1") 
        {
            cout << "Enter your query: ";
            string query;
            getline(cin, query);

            // Подключение к серверу и отправка запроса
            connectToServerAndSendRequest(query);
        } 
        else if (choice == "2") 
        {
            cout << "Exiting client..." << endl;
            break;
        } 
        else 
        {
            cout << "Invalid option. Please select 1 or 2." << endl;
        }
    }
}

int main() 
{
    clientMenu();
    return 0;
}
