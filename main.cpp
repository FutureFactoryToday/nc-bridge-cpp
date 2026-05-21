#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/serial_port.hpp> // Возвращаем заголовок для Serial
#include <iostream>
#include <string>
#include <memory>

// 1. Делаем класс для работы с портом отдельно (Синглтон)
class SerialConnection {
public:
    net::serial_port port;
    SerialConnection(net::io_context& ioc) : port(ioc) {}
};

class CncBridge : public std::enable_shared_from_this<CncBridge> {
    websocket::stream<tcp::socket> ws_;
    SerialConnection& serial_conn_; // Ссылка на общий порт
    beast::flat_buffer buffer_;
    char serial_buf_[1024];

public:
    CncBridge(tcp::socket socket, SerialConnection& sc) 
        : ws_(std::move(socket)), serial_conn_(sc) {}

    void run() {
        ws_.accept();
        std::cout << "[L2] Клиент подключен к общему порту." << std::endl;
        do_read_ws();
        do_read_serial(); // Начинаем слушать порт для этого клиента
    }

private:
    void do_read_ws() {
        ws_.async_read(buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (ec) return; // Просто выходим при дисконнекте

            std::string msg = beast::buffers_to_string(self->buffer_.data());
            self->buffer_.consume(bytes);

            if (!msg.empty() && msg.back() != '\r') msg += "\r";

            // Пишем в ОБЩИЙ порт
            if (self->serial_conn_.port.is_open()) {
                std::cout << "[L3 -> L1] " << msg;
                net::write(self->serial_conn_.port, net::buffer(msg));
            }
            self->do_read_ws();
        });
    }

    void do_read_serial() {
        serial_conn_.port.async_read_some(net::buffer(serial_buf_), 
            [self = shared_from_this()](boost::system::error_code ec, std::size_t n) {
            if (ec) return; // Если порт закроется в main, мы выйдем отсюда
            
            std::string data(self->serial_buf_, n);
            self->ws_.text(true);
            self->ws_.async_write(net::buffer(data), [self](beast::error_code ec, std::size_t) {
                if (!ec) self->do_read_serial();
            });
        });
    }
};

int main() {
    try {
        net::io_context ioc;
        
        // 2. ОТКРЫВАЕМ ПОРТ ОДИН РАЗ ЗДЕСЬ (при старте программы)
        SerialConnection sc(ioc);
        boost::system::error_code ec;
        sc.port.open("/dev/ttyACM0", ec);
        if (ec) {
            std::cerr << "Критическая ошибка порта: " << ec.message() << std::endl;
            return 1;
        }
        sc.port.set_option(net::serial_port_base::baud_rate(9600));

        tcp::acceptor acceptor{ioc, {net::ip::make_address("0.0.0.0"), 8080}};
        std::cout << "L2 запущен. Порт открыт навсегда. Ожидание L3..." << std::endl;

        for (;;) {
            tcp::socket socket(ioc);
            acceptor.accept(socket);
            
            // Запускаем сессию, передавая ссылку на уже открытый порт
            std::make_shared<CncBridge>(std::move(socket), sc)->run();
            
            ioc.run();
            ioc.restart(); 
            std::cout << "[L2] Сессия завершена. Порт остается открытым." << std::endl;
        }
    } catch (std::exception const& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}
