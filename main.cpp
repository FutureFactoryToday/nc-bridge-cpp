#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>
#include <iostream>
#include <string>
#include <memory>

namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

// Класс-менеджер порта
class SerialManager {
public:
    net::serial_port port;
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
                    std::cout << "[L3 -> L1] Отправка: " << msg;
                    boost::asio::write(self->sm_.port, boost::asio::buffer(msg));
                } catch (std::exception& e) {
                    std::cerr << "Ошибка записи в порт: " << e.what() << std::endl;
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
            // Сюда можно добавить отправку данных в WebSocket, если нужно
        }
        start_serial_reading(sm);
    });
}

std::string find_available_port(net::io_context& ioc) {
    std::vector<std::string> port_names;

    #ifdef _WIN32
        // В Windows перебираем COM-порты от 1 до 20
        for (int i = 1; i <= 20; ++i) {
            port_names.push_back("COM" + std::to_string(i));
        }
    #else
        // В Linux проверяем стандартные имена для USB-Serial адаптеров
        for (int i = 0; i < 5; ++i) {
            port_names.push_back("/dev/ttyACM" + std::to_string(i));
            port_names.push_back("/dev/ttyUSB" + std::to_string(i));
        }
    #endif

    for (const auto& name : port_names) {
        try {
            net::serial_port port(ioc);
            port.open(name);
            if (port.is_open()) {
                port.close();
                return name; // Нашли рабочий порт!
            }
        } catch (...) {
            continue; // Порт занят или не существует, идем дальше
        }
    }
    return ""; // Ничего не нашли
}

int main() {
    try {
        net::io_context ioc;
        auto work_guard = net::make_work_guard(ioc);
        auto sm = std::make_shared<SerialManager>(ioc);

        // --- АВТОМАТИЧЕСКИЙ ПОИСК ПОРТА ---
        std::string port_name = find_available_port(ioc);

        if (port_name.empty()) {
            std::cerr << "!!! ОШИБКА: Ни один подходящий последовательный порт не найден." << std::endl;
            // Можно либо выйти, либо продолжить работу сервера без порта
        } else {
            boost::system::error_code ec;
            sm->port.open(port_name, ec);
            if (!ec) {
                sm->port.set_option(net::serial_port_base::baud_rate(9600));
                start_serial_reading(sm);
                std::cout << "[L1] Успешно подключено к: " << port_name << std::endl;
            }
        }

        CncServer server(ioc, *sm);
        std::cout << "[L2] WebSocket сервер запущен на порту 8080." << std::endl;

        ioc.run();

    } catch (std::exception const& e) {
        std::cerr << "FATAL ERROR: " << e.what() << std::endl;
    }
    return 0;
}
