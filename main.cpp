#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>
#include <chrono>
#include <thread>
#include <iostream>
#include <string>
#include <memory>
#include <deque>

#ifndef _WIN32
    #include <sys/ioctl.h>
    #include <filesystem>
#endif

#ifdef _WIN32
    #include <windows.h>
#endif

#define BAUDRATE 115200


namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

// Класс-менеджер порта
class SerialManager {
public:
    net::serial_port port;
    std::string line_accumulator;
    std::mutex accum_mutex;
    char read_buf;
    SerialManager(net::io_context& ioc) : port(ioc) {}
};

// Сессия связи с браузером
class CncSession : public std::enable_shared_from_this<CncSession> {
    websocket::stream<tcp::socket> ws_;
    SerialManager& sm_;
    beast::flat_buffer ws_buffer_;
    std::deque<std::string> write_queue_; 

public:
    CncSession(tcp::socket socket, SerialManager& sm) : ws_(std::move(socket)), sm_(sm) {}

    void start() {
        ws_.async_accept([self = shared_from_this()](beast::error_code ec) {
            if (ec) return;
            std::cout << "[L2] Frontend connect" << std::endl;
            self->send_status(self->sm_.port.is_open());
            self->do_read_ws();
        });
    }

    void send_status(bool port_open) {
        // Создаем строку
        std::string status_msg = port_open ? "STATUS:READY" : "STATUS:NO_DEVICE";
        
        // Захватываем status_msg ПО ЗНАЧЕНИЮ [status_msg]
        // Теперь лямбда хранит внутри себя собственную копию строки
        safe_send(std::move(status_msg));
    }

    void safe_send(std::string msg) {
        auto self = shared_from_this();
        net::post(ws_.get_executor(), [self, msg = std::move(msg)]() {
            bool write_in_progress = !self->write_queue_.empty();
            self->write_queue_.push_back(std::move(msg));
            
            if (!write_in_progress) {
                self->do_write();
            }
        });
    }

    void do_write() {
        auto self = shared_from_this();
        ws_.async_write(net::buffer(write_queue_.front()), 
            [self](beast::error_code ec, std::size_t) {
                if (ec) return;
                self->write_queue_.pop_front();
                if (!self->write_queue_.empty()) {
                    self->do_write();
                }
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
            auto data = self->ws_buffer_.data();
            if (data.size() == 0) {
                self->ws_buffer_.consume(bytes);
                self->do_read_ws();
                return;
            }

            std::string msg = beast::buffers_to_string(data);
            self->ws_buffer_.consume(bytes);

            if (msg.empty()) {
                self->do_read_ws();
                return;
            }

            while(!msg.empty() && (msg.back() == ' ' || msg.back() == '\t')) msg.pop_back();
            // 2. Добавляем терминатор (CR)
            if (!msg.empty() && msg.back() != '\r' && msg.back() != '\n') {
                msg += "\r\n";
            }

            std::cout << "[L3 -> L2] Dispatch: " << msg;

            // 3. СИНХРОННАЯ ЗАПИСЬ (Гарантирует уход в L1 без зависаний)
            if (self->sm_.port.is_open()) {
                try {
                    std::cout << "[L3 -> L1] Dispatch: " << msg;
                    boost::asio::write(self->sm_.port, boost::asio::buffer(msg));
                } catch (...) {
                    std::cerr << "Error writing to serial port: " << std::endl;
                }
            } else {
                std::cout << "[L3] Frontend command ignored (no device): " << msg;
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
void start_serial_reading(std::shared_ptr<SerialManager> sm, std::function<void()> on_error) {
    sm->port.async_read_some(net::buffer(&(sm->read_buf), 1), [sm, on_error](beast::error_code ec, std::size_t n) {
        if (ec) {
            std::cerr << "[L1] Device disconnected: " << ec.message() << std::endl;
            if (sm->port.is_open()) {
                sm->port.close();
            }
            
            if (on_error) on_error(); 
            return; // Выходим из цикла чтения
        }
        else {
            char c = sm->read_buf;

                std::lock_guard<std::mutex> lock(sm->accum_mutex); // Закрыли замок
                if (c == '\r' || c == '\n') {
                    if (!sm->line_accumulator.empty()) {
                        std::string ready_line = std::move(sm->line_accumulator);
                        sm->line_accumulator.clear(); 
                        std::cout << "[L1] Dispatch: " << ready_line << std::endl;
                        //Здесь может быть парсер
                    }
                } else {
                    sm->line_accumulator += c;
                }
        }
        
        // Рекурсивный вызов для следующего символа
        start_serial_reading(sm, on_error);
    });
}

std::string find_available_port(net::io_context& ioc) {
    std::vector<std::string> port_names;

    #ifdef _WIN32
        for (int i = 1; i <= 20; ++i) port_names.push_back("COM" + std::to_string(i));
    #else
        try {
            for (const auto& entry : std::filesystem::directory_iterator("/dev")) {
                std::string s = entry.path().string();
                if (s.find("ttyUSB") != std::string::npos || s.find("ttyACM") != std::string::npos) {
                    port_names.push_back(s);
                }
            }
        } catch (...) {}
    #endif

    std::cout << "[L1] Scanning..." << std::endl;

    for (const auto& name : port_names) {
        try {
            net::serial_port port(ioc);
            port.open(name);
            port.set_option(net::serial_port_base::baud_rate(BAUDRATE));

            std::this_thread::sleep_for(std::chrono::milliseconds(2000));

            // 1. Отправляем запрос
            std::string enter = "\n";
            net::write(port, net::buffer(enter));
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            std::string request = "helo\r";
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
                    std::string response;
                    response.assign(buffer.data(), n);

                    std::cout << "[L1] Checking " << name << ": Received: " << response << std::endl;

                    if (response.find("helo_ok") != std::string::npos) {
                        port.close();
                        return name;
                    }
                }
            } else {
                std::cout << "[L1] Checking " << name << ": No data in buffer" << std::endl;
            }


            port.close(ec);
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
        while (std::getline(std::cin, line)) {
            if (line.empty()) continue;

            // Передаем задачу в io_context
            net::post(ioc, [sm, line]() {
                // Проверяем порт именно В МОМЕНТ выполнения задачи, а не отправки
                if (sm && sm->port.is_open()) {
                    try {
                        std::string msg = line + "\r\n";
                        net::write(sm->port, net::buffer(msg));
                        std::cout << "[Terminal -> L1] Sent: " << line << std::endl;
                    } catch (...) {
                        std::cerr << "[Terminal] Write error!" << std::endl;
                    }
                } else {
                    std::cout << "[Terminal] Command ignored: Device not ready." << std::endl;
                }
            });
        }
    }).detach();

}

void setup_console() 
{
#ifdef _WIN32
    // Включаем UTF-8 в консоли Windows
    SetConsoleOutputCP(65001);
    SetConsoleCP(65001);
    std::setlocale(LC_ALL, "Russian");
#endif
}


int main() {
    try {
        setup_console();
        
        net::io_context ioc;
        //auto work_guard = net::make_work_guard(ioc);
        auto sm = std::make_shared<SerialManager>(ioc);

        // --- АВТОМАТИЧЕСКИЙ ПОИСК ПОРТА ---
        //std::string port_name = find_available_port(ioc);
        //--Заглушка--
        //std::string port_name = "COM5";




        // 2. Функция для периодического поиска
        std::function<void()> do_find_device;
        do_find_device = [&ioc, sm, &do_find_device]() {
            if (sm->port.is_open()) return;

            std::string port_name = find_available_port(ioc);
            
            if (!port_name.empty()) {
                try {
                    sm->port.open(port_name);
                    sm->port.set_option(net::serial_port_base::baud_rate(BAUDRATE));
                    net::write(sm->port, net::buffer("\r\n"));
                    start_serial_reading(sm, do_find_device); 
                    std::cout << "[L1] Successfully connected to: " << port_name << std::endl;
                    return; // Выходим из цикла поиска
                } catch (...) {
                    std::cerr << "[L1] ERROR opening the found port: " << port_name << std::endl;
                }
            }

            // Если не нашли или не открыли — пробуем снова через 3 секунды
            std::cout << "[L1] Searching for device..." << std::endl;
            auto timer = std::make_shared<net::steady_timer>(ioc, std::chrono::seconds(3));
            timer->async_wait([&do_find_device, timer](const boost::system::error_code& ec) {
                if (!ec) do_find_device();
            });
        };

        // Запускаем поиск
        do_find_device();

        start_terminal_input(ioc, sm);
        CncServer server(ioc, *sm);
        std::cout << "[L2] The WebSocket server is running on port 8080." << std::endl;

        ioc.run();

    } catch (std::exception const& e) {
        std::cerr << "FATAL ERROR: " << e.what() << std::endl;
    }
    return 0;
}
