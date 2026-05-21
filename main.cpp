#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>
#include <iostream>
#include <string>
#include <memory>
#include <vector>

namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

// 1. Глобальный или долгоживущий объект порта
class SerialManager {
public:
    net::serial_port port;
    std::vector<char> buffer;
    
    SerialManager(net::io_context& ioc) : port(ioc), buffer(4096) {}
};

// 2. Сессия WebSocket
class CncSession : public std::enable_shared_from_this<CncSession> {
    websocket::stream<tcp::socket> ws_;
    SerialManager& sm_;
    beast::flat_buffer ws_buffer_;

public:
    CncSession(tcp::socket socket, SerialManager& sm) 
        : ws_(std::move(socket)), sm_(sm) {}

    void start() {
        ws_.async_accept([self = shared_from_this()](beast::error_code ec) {
            if (ec) return;
            std::cout << "[L2] Клиент подключен." << std::endl;
            self->do_read_ws();
        });
    }

    void do_read_ws() {
        ws_.async_read(ws_buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (ec) return; // Клиент ушел

            std::string msg = beast::buffers_to_string(self->ws_buffer_.data());
            self->ws_buffer_.consume(bytes);

            if (!msg.empty() && msg.back() != '\r') msg += "\r";

            if (self->sm_.port.is_open()) {
                std::cout << "[L3 -> L1] " << msg;
                net::write(self->sm_.port, net::buffer(msg));
            }
            self->do_read_ws();
        });
    }

    // Метод для отправки данных из порта клиенту
    void deliver(const std::string& data) {
        auto msg = std::make_shared<std::string>(data);
        ws_.async_write(net::buffer(*msg), [self = shared_from_this(), msg](beast::error_code ec, std::size_t) {
            // Обработка ошибок записи
        });
    }
};

// 3. Сервер, который не спит
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
            if (!ec) {
                std::make_shared<CncSession>(std::move(socket), sm_)->start();
            }
            do_accept(); // Сразу ждем следующего, не блокируя ioc
        });
    }
};

// 4. Поток чтения порта (работает всегда)
void start_serial_reading(SerialManager& sm) {
    sm.port.async_read_some(net::buffer(sm.buffer), [&](beast::error_code ec, std::size_t n) {
        if (!ec) {
            // Здесь в будущем можно добавить рассылку всем клиентам
            // Для теста просто выводим в консоль
            // std::cout << "[L1 -> L2] " << std::string(sm.buffer.data(), n) << std::endl;
            start_serial_reading(sm);
        }
    });
}

int main() {
    try {
        net::io_context ioc;
        SerialManager sm(ioc);

        boost::system::error_code ec;
        sm.port.open("/dev/ttyACM0", ec);
        if (ec) {
            std::cerr << "ОШИБКА ПОРТА: " << ec.message() << std::endl;
            return 1;
        }
        sm.port.set_option(net::serial_port_base::baud_rate(9600));

        CncServer server(ioc, sm);
        start_serial_reading(sm);

        std::cout << "L2 запущен. Порт открыт. Нажмите Ctrl+C для выхода." << std::endl;
        
        // ВАЖНО: ioc.run() один раз на всю жизнь программы
        ioc.run();

    } catch (std::exception const& e) {
        std::cerr << "FATAL: " << e.what() << std::endl;
    }
    return 0;
}
