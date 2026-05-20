#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/serial_port.hpp> // Возвращаем заголовок для Serial
#include <iostream>
#include <string>
#include <memory>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

class CncBridge : public std::enable_shared_from_this<CncBridge> {
    websocket::stream<tcp::socket> ws_;
    net::serial_port serial_; // Объект порта возвращен
    beast::flat_buffer buffer_;
    char serial_buf_[1024];    // Буфер для чтения из Serial

public:
    // В конструктор добавляем инициализацию serial_ через io_context сокета
    explicit CncBridge(tcp::socket socket) 
        : ws_(std::move(socket)), 
          serial_(ws_.get_executor()) {}

    void run(std::string dev_path) {
        // --- СЕКЦИЯ ПОДКЛЮЧЕНИЯ К ЖЕЛЕЗУ (L1) ---
        /* 
        boost::system::error_code ec;
        serial_.open(dev_path, ec);
        if (!ec) {
            serial_.set_option(net::serial_port_base::baud_rate(115200));
            std::cout << "[L2] Успешно подключено к " << dev_path << std::endl;
            do_read_serial(); // Запуск чтения из железа
        } else {
            std::cout << "[L2] Предупреждение: Порт не открыт (" << ec.message() << ")" << std::endl;
        }
        */

        ws_.accept();
        std::cout << "[L2] Frontend подключен. Режим отладки (Loopback) активен." << std::endl;
        do_read_ws();
    }

private:
    // Чтение из Браузера (L3) -> Запись в Железо (L1) или Loopback
    void do_read_ws() {
        ws_.async_read(buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (!ec) {
                std::string msg = beast::buffers_to_string(self->buffer_.data());
                std::cout << "[L3 -> L2] Команда: " << msg << std::endl;

                // Если порт открыт — отправляем в станок
                if (self->serial_.is_open()) {
                    net::write(self->serial_, net::buffer(msg));
                }

                // --- LOOPBACK ЭМУЛЯЦИЯ ---
                // Отправляем эхо назад во фронтенд
                self->ws_.text(self->ws_.got_text());
                self->ws_.async_write(self->buffer_.data(), [self](beast::error_code ec, std::size_t) {
                    self->buffer_.consume(self->buffer_.size());
                    if (!ec) self->do_read_ws();
                });
            }
        });
    }

    // Чтение из Железа (L1) -> Запись в Браузер (L3)
    void do_read_serial() {
        serial_.async_read_some(net::buffer(serial_buf_), 
            [self = shared_from_this()](boost::system::error_code ec, std::size_t n) {
            if (!ec) {
                // Пересылаем данные из порта в WebSocket
                std::string data(self->serial_buf_, n);
                self->ws_.async_write(net::buffer(data), [self](beast::error_code ec, std::size_t) {
                    if (!ec) self->do_read_serial();
                });
            }
        });
    }
};

int main() {
    try {
        net::io_context ioc;
        tcp::acceptor acceptor{ioc, {net::ip::make_address("127.0.0.1"), 8080}};
        
        std::cout << "C++ L2-Daemon (Hybrid: Loopback/Serial ready)" << std::endl;

        while (true) {
            tcp::socket socket{ioc};
            acceptor.accept(socket);
            // Передаем путь к устройству (пока закомментировано внутри)
            std::make_shared<CncBridge>(std::move(socket))->run("/dev/ttyACM0");
            ioc.run();
            ioc.restart(); 
        }
    } catch (std::exception const& e) {
        std::cerr << "Fatal Error: " << e.what() << std::endl;
    }
    return 0;
}
