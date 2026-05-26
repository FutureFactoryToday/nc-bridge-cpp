#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>
#include <chrono>
#include <thread>
#include <iostream>
#include <string>
#include <memory>

#ifndef _WIN32
    #include <sys/ioctl.h>
#endif


namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

// Класс-менеджер порта
class SerialManager {
public:
    net::serial_port port;
    std::string line_accumulator;
    char read_buf;
    SerialManager(net::io_context& ioc) : port(ioc) {}
};

// Сессия связи с браузером
class CncSession : public std::enable_shared_from_this<CncSession> {
    websocket::stream<tcp::socket> ws_;
    SerialManager& sm_;
    beast::flat_buffer ws_buffer_;

public:
    CncSession(tcp::socket socket, SerialManager& sm) : ws_(std::move(socket)), sm_(sm) {}

    void start() {
        ws_.async_accept([self = shared_from_this()](beast::error_code ec) {
            if (ec) return;
            std::cout << "[L2] Frontend connect" << std::endl;
            self->do_read_ws();
        });
    }

private:
    void do_read_ws() {
        ws_.async_read(ws_buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (ec) {
                std::cout << "[L2] Frontend disconnect: " << ec.message() << std::endl;
                return;
            }

            // 1. Получаем строку
            std::string msg = beast::buffers_to_string(self->ws_buffer_.data());
            self->ws_buffer_.consume(bytes);

            // 2. Добавляем терминатор (CR)
            if (!msg.empty() && msg.back() != '\r') msg += "\r";

            // 3. СИНХРОННАЯ ЗАПИСЬ (Гарантирует уход в L1 без зависаний)
            if (self->sm_.port.is_open()) {
                try {
                    std::cout << "[L3 -> L1] Dispatch: " << msg;
                    boost::asio::write(self->sm_.port, boost::asio::buffer(msg));
                } catch (std::exception& e) {
                    std::cerr << "Error writing to port: " << e.what() << std::endl;
                }
            }

            // Продолжаем слушать
            self->do_read_ws();
        });
    }

};

// Сервер
class CncServer {
    tcp::acceptor acceptor_;
    SerialManager& sm_;

public:
    CncServer(net::io_context& ioc, SerialManager& sm) 
        : acceptor_(ioc, {net::ip::make_address("0.0.0.0"), 8080}), sm_(sm) {
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept([this](beast::error_code ec, tcp::socket socket) {
            if (!ec) std::make_shared<CncSession>(std::move(socket), sm_)->start();
            do_accept();
        });
    }
};


// Постоянное чтение порта
void start_serial_reading(std::shared_ptr<SerialManager> sm) {
    sm->port.async_read_some(net::buffer(&(sm->read_buf), 1), [sm](beast::error_code ec, std::size_t n) {
        if (!ec) {
            char c = sm->read_buf;

            if (c == '\r' || c == '\n') {
                if (!sm->line_accumulator.empty()) {
                    // Строка готова для парсера
                    std::cout << "[L1 -> Terminal] Dispatch: " << sm->line_accumulator << std::endl;
                    
                    // ВЫЗОВ ВАШЕГО ПАРСЕРА ЗДЕСЬ:
                    // sm->parser.process(sm->line_accumulator);

                    sm->line_accumulator.clear(); // Очистка для новой строки
                }
            } else {
                sm->line_accumulator += c;
            }
        }
        
        // Рекурсивный вызов для следующего символа
        start_serial_reading(sm);
    });
}

std::string find_available_port(net::io_context& ioc) {
    std::vector<std::string> port_names;

    #ifdef _WIN32
        for (int i = 1; i <= 20; ++i) port_names.push_back("COM" + std::to_string(i));
    #else
        for (int i = 0; i < 5; ++i) {
            port_names.push_back("/dev/ttyACM" + std::to_string(i));
            port_names.push_back("/dev/ttyUSB" + std::to_string(i));
        }
    #endif

    std::cout << "[L1] Scanning..." << std::endl;

    for (const auto& name : port_names) {
        try {
            net::serial_port port(ioc);
            port.open(name);
            port.set_option(net::serial_port_base::baud_rate(9600));

            // 1. Отправляем запрос
            std::string request = "cs_helo\r\n";
            net::write(port, net::buffer(request));

            // 2. Ждем ответа (на Linux/Arduino лучше 500ms, т.к. бывает авто-ресет)
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            // 3. ПРОВЕРКА БУФЕРА
            boost::system::error_code ec;
            size_t bytes_to_read = 0;

            #ifdef _WIN32
                COMSTAT status;
                DWORD errors;
                if (ClearCommError(port.native_handle(), &errors, &status)) {
                    bytes_to_read = status.cbInQue;
                }
            #else
                int available = 0;
                if (::ioctl(port.native_handle(), FIONREAD, &available) >= 0) {
                    bytes_to_read = static_cast<size_t>(available);
                }
            #endif

            if (bytes_to_read > 0) {
                std::vector<char> buffer(bytes_to_read);
                size_t n = port.read_some(net::buffer(buffer), ec);
                if (!ec && n > 0) {
                    std::string response(buffer.data(), n);
                    std::cout << "[L1] Checking " << name << ": Received: " << response << std::endl;
                    if (response.find("helo_ok") != std::string::npos) {
                        port.close();
                        return name;
                    }
                }
            } else {
                std::cout << "[L1] Checking " << name << ": No data in buffer" << std::endl;
            }


            port.close();
        } catch (...) {
            continue; // Порт занят или не существует
        }
    }

    std::cerr << "[L2] ERROR: Device 'helo_ok' NOT FOUND!" << std::endl;
    return ""; 
}


void start_terminal_input(net::io_context& ioc, std::shared_ptr<SerialManager> sm) {
    // Запускаем поток для чтения cin
    std::thread([&ioc, sm]() {
        std::string line;
        std::cout << "[Debug] Terminal input ready. Type commands here:" << std::endl;
        
        while (std::getline(std::cin, line)) {
            if (line.empty()) continue;

            // Добавляем CR, как в логике с WebSocket
            if (line.back() != '\r') line += "\r";

            // Передаем выполнение в поток io_context, чтобы не было конфликтов с портом
            net::post(ioc, [sm, line]() {
                if (sm->port.is_open()) {
                    try {
                        std::cout << "[Terminal -> L1] Dispatch: " << line << "\n";
                        boost::asio::write(sm->port, net::buffer(line));
                    } catch (std::exception& e) {
                        std::cerr << "Terminal write error: " << e.what() << std::endl;
                    }
                } else {
                    std::cerr << "[L1] Error: Port is closed. Cannot send: " << line << std::endl;
                }
            });
        }
    }).detach(); // Отсоединяем поток, чтобы он жил своей жизнью
}


int main() {
    try {
        net::io_context ioc;
        auto work_guard = net::make_work_guard(ioc);
        auto sm = std::make_shared<SerialManager>(ioc);

        // --- АВТОМАТИЧЕСКИЙ ПОИСК ПОРТА ---
        //std::string port_name = find_available_port(ioc);
        //--Заглушка--
        std::string port_name = "COM4";

        if (port_name.empty()) {
            std::cerr << "!!! ERROR: No suitable serial port found." << std::endl;
            // Можно либо выйти, либо продолжить работу сервера без порта
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            boost::system::error_code ec;
            sm->port.open(port_name, ec);
            if (!ec) {
                sm->port.set_option(net::serial_port_base::baud_rate(9600));
                sm->port.set_option(net::serial_port_base::flow_control(net::serial_port_base::flow_control::none));
                start_serial_reading(sm);
                std::cout << "[L1] Successfully connected to: " << port_name << std::endl;
            } else {
                std::cerr << "[L1] ERROR opening the found port: " << ec.message() << std::endl;
            }
        }

        start_terminal_input(ioc, sm);

        CncServer server(ioc, *sm);
        std::cout << "[L2] The WebSocket server is running on port 8080." << std::endl;

        ioc.run();

    } catch (std::exception const& e) {
        std::cerr << "FATAL ERROR: " << e.what() << std::endl;
    }
    return 0;
}
