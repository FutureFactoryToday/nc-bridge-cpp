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
            std::cout << "[L2] Frontend подключен" << std::endl;
            self->do_read_ws();
        });
    }

private:
    void do_read_ws() {
        ws_.async_read(ws_buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (ec) {
                std::cout << "[L2] Frontend отключился: " << ec.message() << std::endl;
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

int main() {
    try {
        net::io_context ioc;
        
        // --- ВОТ ЭТО РЕШАЕТ ПРОБЛЕМУ ---
        // work_guard не дает ioc.run() завершиться, даже когда нет задач
        auto work_guard = net::make_work_guard(ioc);

        auto sm = std::make_shared<SerialManager>(ioc);
        boost::system::error_code ec;
        sm->port.open("/dev/ttyACM0", ec);
        if (!ec) {
            sm->port.set_option(net::serial_port_base::baud_rate(9600));
            start_serial_reading(sm);
        } else {
            std::cerr << "Предупреждение: Порт не найден, но сервер запущен." << std::endl;
        }

        CncServer server(ioc, *sm);
        std::cout << "[L2] Демон активен. Нажмите Ctrl+C для выхода." << std::endl;

        // Теперь ioc.run() будет работать ВЕЧНО
        ioc.run();

    } catch (std::exception const& e) {
        std::cerr << "FATAL: " << e.what() << std::endl;
    }
    return 0;
}
