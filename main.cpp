#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>
#include <iostream>
#include <string>
#include <memory>
#include <termios.h> // Для низкоуровневой очистки порта

namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

class SerialManager {
public:
    net::serial_port port;
    char read_buf[1024];
    
    SerialManager(net::io_context& ioc) : port(ioc) {}

    // Метод для очистки системных буферов порта
    void flush() {
        if (port.is_open()) {
            tcflush(port.lowest_layer().native_handle(), TCIOFLUSH);
        }
    }
};

class CncSession : public std::enable_shared_from_this<CncSession> {
    websocket::stream<tcp::socket> ws_;
    SerialManager& sm_;
    beast::flat_buffer ws_buffer_;

public:
    CncSession(tcp::socket socket, SerialManager& sm) : ws_(std::move(socket)), sm_(sm) {}

    void start() {
        // При новом подключении — чистим порт от старых "хвостов"
        sm_.flush();
        
        ws_.async_accept([self = shared_from_this()](beast::error_code ec) {
            if (ec) return;
            std::cout << "[L2] Клиент подключен. Порт очищен." << std::endl;
            self->do_read_ws();
        });
    }

private:
    void do_read_ws() {
        ws_.async_read(ws_buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (ec) {
                std::cout << "[L2] Клиент ушел." << std::endl;
                return;
            }

            std::string msg = beast::buffers_to_string(self->ws_buffer_.data());
            self->ws_buffer_.consume(bytes);

            // Гарантируем \r
            if (!msg.empty() && msg.back() != '\r') msg += "\r";

            if (self->sm_.port.is_open()) {
                // Используем async_write вместо синхронного write для порта
                net::async_write(self->sm_.port, net::buffer(msg), 
                    [msg](boost::system::error_code ec_w, std::size_t) {
                        if (!ec_w) std::cout << "[L3 -> L1] Отправлено: " << msg;
                    });
            }
            self->do_read_ws();
        });
    }
};

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
            do_accept();
        });
    }
};

// Функция чтения порта, которая никогда не останавливается
void start_serial_reading(std::shared_ptr<SerialManager> sm) {
    sm->port.async_read_some(net::buffer(sm->read_buf), 
        [sm](boost::system::error_code ec, std::size_t n) {
            if (!ec && n > 0) {
                // Данные от STM32 можно выводить в консоль для отладки
                // std::cout << "[L1 -> L2]: " << std::string(sm->read_buf, n) << std::endl;
            }
            start_serial_reading(sm);
        });
}

int main() {
    try {
        net::io_context ioc;
        auto sm = std::make_shared<SerialManager>(ioc);

        boost::system::error_code ec;
        sm->port.open("/dev/ttyACM0", ec);
        if (ec) {
            std::cerr << "ОШИБКА ПОРТА: " << ec.message() << std::endl;
            return 1;
        }

        // Настройка порта
        sm->port.set_option(net::serial_port_base::baud_rate(9600));
        sm->port.set_option(net::serial_port_base::flow_control(net::serial_port_base::flow_control::none));
        sm->port.set_option(net::serial_port_base::parity(net::serial_port_base::parity::none));
        sm->port.set_option(net::serial_port_base::stop_bits(net::serial_port_base::stop_bits::one));

        CncServer server(ioc, *sm);
        start_serial_reading(sm);

        std::cout << "[L2] Демон готов. Ожидание L3..." << std::endl;
        
        // Запускаем ioc в нескольких потоках для стабильности (опционально)
        // Но для начала хватит и одного
        ioc.run();

    } catch (std::exception const& e) {
        std::cerr << "FATAL: " << e.what() << std::endl;
    }
    return 0;
}
