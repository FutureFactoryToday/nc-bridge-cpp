#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>
#include <iostream>
#include <string>
#include <memory>
#include <vector>

// Сокращения для удобства
namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

// 1. Класс для управления единственным соединением с STM32
class SerialConnection {
public:
    net::serial_port port;
    // Буфер для чтения из железа
    std::vector<char> serial_data;

    SerialConnection(net::io_context& ioc) : port(ioc), serial_data(4096) {}
};

// 2. Класс сессии WebSocket (L2 <-> L3)
class CncBridge : public std::enable_shared_from_this<CncBridge> {
    websocket::stream<tcp::socket> ws_;
    SerialConnection& sc_;
    beast::flat_buffer ws_buffer_;

public:
    CncBridge(tcp::socket socket, SerialConnection& sc) 
        : ws_(std::move(socket)), sc_(sc) {}

    void run() {
        // Настройка параметров WebSocket (таймауты и т.д.)
        ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
        
        // Принимаем соединение
        ws_.async_accept([self = shared_from_this()](beast::error_code ec) {
            if (ec) return;
            std::cout << "[L2] Клиент L3 подключен к общему порту." << std::endl;
            self->do_read_ws();
            self->do_read_serial(); 
        });
    }

private:
    // Чтение из Браузера (L3) -> Запись в STM32 (L1)
    void do_read_ws() {
        ws_.async_read(ws_buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (ec) {
                std::cout << "[L2] Клиент отключился." << std::endl;
                return; 
            }

            std::string msg = beast::buffers_to_string(self->ws_buffer_.data());
            self->ws_buffer_.consume(bytes);

            // Добавляем \r (CR), если его нет
            if (!msg.empty() && msg.back() != '\r') {
                msg += "\r";
            }

            if (self->sc_.port.is_open()) {
                std::cout << "[L3 -> L1] Отправка: " << msg;
                // Синхронная запись в порт (для команд это надежнее)
                net::write(self->sc_.port, net::buffer(msg));
            }

            self->do_read_ws();
        });
    }

    // Чтение из STM32 (L1) -> Запись в Браузер (L3)
    void do_read_serial() {
        // Используем async_read_some для получения данных из порта
        self_read_serial();
    }

    void self_read_serial() {
        sc_.port.async_read_some(net::buffer(sc_.serial_data), 
            [self = shared_from_this()](boost::system::error_code ec, std::size_t n) {
            if (ec) {
                // Если порт закрыт или ошибка, прекращаем чтение для этой сессии
                return;
            }

            if (n > 0) {
                std::string data(self->sc_.serial_data.data(), n);
                // Пересылаем данные в WebSocket как текст
                self->ws_.text(true);
                self->ws_.async_write(net::buffer(data), [self](beast::error_code ec_ws, std::size_t) {
                    if (!ec_ws) self->self_read_serial();
                });
            } else {
                self->self_read_serial();
            }
        });
    }
};

int main() {
    try {
        net::io_context ioc;

        // Инициализируем порт ОДИН раз
        SerialConnection sc(ioc);
        boost::system::error_code ec_serial;
        
        // ВАЖНО: Проверь путь к порту (/dev/ttyACM0 или /dev/ttyUSB0)
        sc.port.open("/dev/ttyACM0", ec_serial);
        if (ec_serial) {
            std::cerr << "ОШИБКА ПОРТА: " << ec_serial.message() << std::endl;
            return 1;
        }
        
        sc.port.set_option(net::serial_port_base::baud_rate(9600));
        sc.port.set_option(net::serial_port_base::flow_control(net::serial_port_base::flow_control::none));

        tcp::acceptor acceptor{ioc, {net::ip::make_address("0.0.0.0"), 8080}};
        std::cout << "L2 запущен (9600 baud). Ожидание подключений на порту 8080..." << std::endl;

        // Цикл приема новых клиентов
        while (true) {
            tcp::socket socket(ioc);
            acceptor.accept(socket);
            
            // Создаем и запускаем сессию
            std::make_shared<CncBridge>(std::move(socket), sc)->run();
            
            // Запускаем обработку событий до тех пор, пока клиент не отключится
            ioc.run();
            
            // После отключения клиента сбрасываем контекст для следующего
            ioc.restart();
            std::cout << "[L2] Сессия завершена. Ожидание нового клиента..." << std::endl;
        }
    } catch (std::exception const& e) {
        std::cerr << "FATAL ERROR: " << e.what() << std::endl;
        return 1;
    }
}
