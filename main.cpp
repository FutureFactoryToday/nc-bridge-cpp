#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio.hpp>
#include <functional>
#include <vector>
#include <chrono>
#include <thread>
#include <iostream>
#include <string>
#include <memory>
#include <deque>
#include <atomic>
#include <mutex>
#include <boost/asio/signal_set.hpp>

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

// ----------------------------------------------------------------------
//  SerialManager – управление последовательным портом, уведомления о статусе и новых строках
// ----------------------------------------------------------------------
class SerialManager : public std::enable_shared_from_this<SerialManager> {
public:
    SerialManager(net::io_context& ioc) : port(ioc) {}

    // Публичные методы доступа к внутренним данным
    net::serial_port& get_port() { return port; }

    std::mutex& get_accum_mutex() { return accum_mutex; }
    std::string& get_line_accumulator() { return line_accumulator; }
    char& get_read_buf() { return read_buf; }

    bool get_was_open() const { return was_open; }
    void set_was_open(bool val) { was_open = val; }

    bool acquire_search_lock() {
        bool expected = false;
        return is_searching.compare_exchange_strong(expected, true);
    }
    void release_search_lock() {
        is_searching = false;
    }

    bool is_stop_requested() const { return stop_flag; }
    void set_stop_requested() { stop_flag = true; }

    // Методы для подписки на события
    void add_status_observer(std::function<void(bool)> observer) {
        std::lock_guard<std::mutex> lock(status_observers_mutex);
        status_observers.push_back(std::move(observer));
    }

    void add_line_observer(std::function<void(const std::string&)> observer) {
        std::lock_guard<std::mutex> lock(line_observers_mutex);
        line_observers.push_back(std::move(observer));
    }

    // Уведомления
    void notify_status_change(bool is_open) {
        std::lock_guard<std::mutex> lock(status_observers_mutex);
        for (auto& obs : status_observers) obs(is_open);
    }

    void notify_line_received(const std::string& line) {
        std::lock_guard<std::mutex> lock(line_observers_mutex);
        for (auto& obs : line_observers) obs(line);
    }

    // Проверка и уведомление о смене статуса порта
    void check_and_notify_status() {
        bool current_state = port.is_open();
        if (current_state != was_open) {
            was_open = current_state;
            notify_status_change(current_state);
        }
    }

    // Остановка всех операций порта
    void stop() {
        set_stop_requested();
        boost::system::error_code ec;
        port.cancel(ec);   // отменяем все асинхронные операции
        port.close(ec);    // закрываем порт
        notify_status_change(false);
    }

private:
    net::serial_port port;
    std::string line_accumulator;
    std::mutex accum_mutex;
    char read_buf = 0;
    bool was_open = false;
    std::atomic<bool> is_searching{false};
    std::atomic<bool> stop_flag{false};   // флаг остановки всех операций

    // Наблюдатели
    std::mutex status_observers_mutex;
    std::vector<std::function<void(bool)>> status_observers;
    std::mutex line_observers_mutex;
    std::vector<std::function<void(const std::string&)>> line_observers;
};

// ----------------------------------------------------------------------
//  CncSession – WebSocket-сессия для одного фронтенда
// ----------------------------------------------------------------------
class CncSession : public std::enable_shared_from_this<CncSession> {
    websocket::stream<tcp::socket> ws_;
    SerialManager& sm_;
    beast::flat_buffer ws_buffer_;
    std::deque<std::string> write_queue_;
    std::shared_ptr<net::steady_timer> status_timer_;
    std::atomic<bool> status_timer_active_{false};

public:
    CncSession(tcp::socket socket, SerialManager& sm)
        : ws_(std::move(socket)), sm_(sm) {}

    void init() {
        sm_.add_status_observer([weak_self = std::weak_ptr<CncSession>(shared_from_this())](bool is_open) {
            if (auto self = weak_self.lock())
                self->send_status(is_open);
        });
    }

    void start() {
        ws_.async_accept([self = shared_from_this()](beast::error_code ec) {
            if (ec) {
                std::cout << "[L2] Accept error: " << ec.message() << std::endl;
                return;
            }
            std::cout << "[L2] Frontend connected" << std::endl;
            self->send_status(self->sm_.get_port().is_open());
            self->start_periodic_status();
            self->do_read_ws();
        });
    }

    void deliver(const std::string& message) {
        safe_send(message);
    }

    // Принудительное закрытие сессии (для остановки)
    void stop() {
        boost::system::error_code ec;
        ws_.close(websocket::close_code::normal, ec);
        stop_periodic_status();
    }

private:
    void send_status(bool port_open) {
        std::string status_msg = port_open ? "STATUS:READY" : "STATUS:NO_DEVICE";
        safe_send(std::move(status_msg));
    }

    void safe_send(std::string msg) {
        auto self = shared_from_this();
        net::post(ws_.get_executor(), [self, msg = std::move(msg)]() {
            if (!self->ws_.is_open()) return;
            bool write_in_progress = !self->write_queue_.empty();
            self->write_queue_.push_back(std::move(msg));
            if (!write_in_progress)
                self->do_write();
        });
    }

    void do_write() {
        auto self = shared_from_this();
        ws_.async_write(net::buffer(write_queue_.front()),
            [self](beast::error_code ec, std::size_t) {
                if (ec) {
                    std::cout << "[L2] Write error: " << ec.message() << std::endl;
                    return;
                }
                self->write_queue_.pop_front();
                if (!self->write_queue_.empty())
                    self->do_write();
            });
    }

    void start_periodic_status() {
        if (status_timer_active_.exchange(true))
            return;
        schedule_status_timer();
    }

    void schedule_status_timer() {
        if (!status_timer_active_)
            return;

        auto self = shared_from_this();
        status_timer_ = std::make_shared<net::steady_timer>(ws_.get_executor());
        status_timer_->expires_after(std::chrono::seconds(5));
        status_timer_->async_wait([self](boost::system::error_code ec) {
            if (ec == boost::asio::error::operation_aborted)
                return;
            if (!ec && self->ws_.is_open() && self->status_timer_active_ && !self->sm_.is_stop_requested()) {
                self->send_status(self->sm_.get_port().is_open());
                self->schedule_status_timer();
            } else {
                self->status_timer_active_ = false;
            }
        });
    }

    void stop_periodic_status() {
        status_timer_active_ = false;
        if (status_timer_)
            status_timer_->cancel();
    }

    void do_read_ws() {
        ws_.async_read(ws_buffer_, [self = shared_from_this()](beast::error_code ec, std::size_t bytes) {
            if (ec) {
                std::cout << "[L2] Frontend disconnected: " << ec.message() << std::endl;
                self->stop_periodic_status();
                return;
            }
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
            while (!msg.empty() && (msg.back() == ' ' || msg.back() == '\t'))
                msg.pop_back();
            if (!msg.empty() && msg.back() != '\r' && msg.back() != '\n')
                msg += "\r\n";

            std::cout << "[L3 -> L2] Dispatch: " << msg;

            if (self->sm_.get_port().is_open() && !self->sm_.is_stop_requested()) {
                try {
                    std::cout << "[L3 -> L1] Dispatch: " << msg;
                    auto write_buffer = std::make_shared<std::string>(msg);
                    net::async_write(self->sm_.get_port(), net::buffer(*write_buffer),
                        [write_buffer](boost::system::error_code ec, std::size_t) {
                            if (ec)
                                std::cerr << "Error writing to serial port: " << ec.message() << std::endl;
                        });
                } catch (const std::exception& e) {
                    std::cerr << "Exception writing to serial port: " << e.what() << std::endl;
                }
            } else {
                std::cout << "[L3] Frontend command ignored (no device or stopping): " << msg;
            }
            self->do_read_ws();
        });
    }

public:
    ~CncSession() {
        stop_periodic_status();
        std::cout << "[L2] Session destroyed" << std::endl;
    }
};

// ----------------------------------------------------------------------
//  CncServer – принимает подключения и управляет списком сессий
// ----------------------------------------------------------------------
class CncServer : public std::enable_shared_from_this<CncServer> {
    tcp::acceptor acceptor_;
    SerialManager& sm_;
    std::vector<std::weak_ptr<CncSession>> sessions_;
    std::mutex sessions_mutex_;
    std::atomic<bool> stop_flag_{false};

public:
    CncServer(net::io_context& ioc, SerialManager& sm)
        : acceptor_(ioc, {net::ip::make_address("0.0.0.0"), 8080}), sm_(sm)
    {
        do_accept();
    }

    void init() {
        auto self = shared_from_this();
        sm_.add_line_observer([self](const std::string& line) {
            if (self && !self->stop_flag_)
                self->broadcast(line);
        });
    }

    void broadcast(const std::string& message) {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(),
            [](const std::weak_ptr<CncSession>& wp) { return wp.expired(); }),
            sessions_.end());

        for (auto& wp : sessions_) {
            if (auto session = wp.lock())
                session->deliver(message);
        }
    }

    // Остановка сервера: закрываем acceptor и все сессии
    void stop() {
        stop_flag_ = true;
        boost::system::error_code ec;
        acceptor_.close(ec);
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        for (auto& wp : sessions_) {
            if (auto session = wp.lock())
                session->stop();
        }
        sessions_.clear();
    }

private:
    void do_accept() {
        acceptor_.async_accept([this](beast::error_code ec, tcp::socket socket) {
            if (stop_flag_) return;
            if (!ec) {
                auto session = std::make_shared<CncSession>(std::move(socket), sm_);
                session->init();
                session->start();
                {
                    std::lock_guard<std::mutex> lock(sessions_mutex_);
                    sessions_.push_back(session);
                }
            }
            if (!stop_flag_)
                do_accept();
        });
    }
};

// ----------------------------------------------------------------------
//  Постоянное асинхронное чтение из последовательного порта (с учётом флага остановки)
// ----------------------------------------------------------------------
void start_serial_reading(std::shared_ptr<SerialManager> sm, std::function<void()> on_disconnect) {
    if (!sm->get_port().is_open()) return;

    auto read_loop = std::make_shared<std::function<void()>>();
    *read_loop = [sm, on_disconnect, read_loop]() {
        if (!sm->get_port().is_open() || sm->is_stop_requested()) return;

        sm->get_port().async_read_some(net::buffer(&(sm->get_read_buf()), 1),
            [sm, on_disconnect, read_loop](beast::error_code ec, std::size_t n) {
                if (sm->is_stop_requested()) return;
                if (ec) {
                    std::cerr << "[L1] Device disconnected: " << ec.message() << std::endl;
                    if (sm->get_port().is_open()) {
                        boost::system::error_code close_ec;
                        sm->get_port().close(close_ec);
                        sm->set_was_open(false);
                        sm->notify_status_change(false);
                    }
                    if (on_disconnect && !sm->is_stop_requested()) {
                        auto executor = sm->get_port().get_executor();
                        net::post(executor, [on_disconnect]() { on_disconnect(); });
                    }
                    return;
                }

                char c = sm->get_read_buf();
                {
                    std::lock_guard<std::mutex> lock(sm->get_accum_mutex());
                    if (c == '\r' || c == '\n') {
                        if (!sm->get_line_accumulator().empty()) {
                            std::string ready_line = std::move(sm->get_line_accumulator());
                            sm->get_line_accumulator().clear();
                            std::cout << "[L1] Dispatch: " << ready_line << std::endl;
                            sm->notify_line_received(ready_line);
                        }
                    } else {
                        sm->get_line_accumulator() += c;
                    }
                }

                if (!sm->is_stop_requested())
                    (*read_loop)();
            });
    };
    (*read_loop)();
}

// ----------------------------------------------------------------------
//  Поиск устройства по протоколу "helo_ok" (с учётом флага остановки)
// ----------------------------------------------------------------------
std::string find_available_port(net::io_context& ioc) {
    std::vector<std::string> port_names;

#ifdef _WIN32
    for (int i = 1; i <= 20; ++i) port_names.push_back("COM" + std::to_string(i));
#else
    try {
        for (const auto& entry : std::filesystem::directory_iterator("/dev")) {
            std::string s = entry.path().string();
            if (s.find("ttyUSB") != std::string::npos || s.find("ttyACM") != std::string::npos)
                port_names.push_back(s);
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

            std::string enter = "\n";
            net::write(port, net::buffer(enter));
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            std::string request = "helo\r";
            net::write(port, net::buffer(request));
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            boost::system::error_code ec;
            size_t bytes_to_read = 0;

#ifdef _WIN32
            COMSTAT status;
            DWORD errors;
            if (ClearCommError(port.native_handle(), &errors, &status))
                bytes_to_read = status.cbInQue;
#else
            int available = 0;
            if (::ioctl(port.native_handle(), FIONREAD, &available) >= 0)
                bytes_to_read = static_cast<size_t>(available);
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
            port.close(ec);
        } catch (...) {
            continue;
        }
    }

    std::cerr << "[L1] ERROR: Device 'helo_ok' NOT FOUND!" << std::endl;
    return "";
}

// ----------------------------------------------------------------------
//  Фоновое чтение из stdin (терминал) – тоже учитываем флаг остановки
// ----------------------------------------------------------------------
void start_terminal_input(net::io_context& ioc, std::shared_ptr<SerialManager> sm) {
    std::thread([&ioc, sm]() {
        std::string line;
        while (std::getline(std::cin, line)) {
            if (sm->is_stop_requested()) break;
            if (line.empty()) continue;
            net::post(ioc, [sm, line]() {
                if (sm->is_stop_requested()) return;
                if (sm && sm->get_port().is_open()) {
                    try {
                        std::string msg = line + "\r\n";
                        net::write(sm->get_port(), net::buffer(msg));
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

// ----------------------------------------------------------------------
//  Циклический поиск устройства (с учётом флага остановки)
// ----------------------------------------------------------------------
void do_find_device(std::shared_ptr<SerialManager> sm, net::io_context& ioc, std::shared_ptr<net::steady_timer> timer = nullptr) {
    if (sm->is_stop_requested()) return;
    if (sm->get_port().is_open()) return;

    if (!sm->acquire_search_lock()) return;

    std::string port_name = find_available_port(ioc);

    if (!port_name.empty()) {
        try {
            sm->get_port().open(port_name);
            sm->get_port().set_option(net::serial_port_base::baud_rate(BAUDRATE));
            sm->set_was_open(true);

            try {
                net::write(sm->get_port(), net::buffer("\r\n"));
            } catch (const std::exception& e) {
                std::cerr << "[L1] Initial write failed: " << e.what() << std::endl;
                throw;
            }

            auto sm_ptr = sm;
            start_serial_reading(sm, [&ioc, sm_ptr]() {
                if (sm_ptr->is_stop_requested()) return;
                auto new_timer = std::make_shared<net::steady_timer>(ioc, std::chrono::seconds(1));
                do_find_device(sm_ptr, ioc, new_timer);
            });

            sm->notify_status_change(true);
            std::cout << "[L1] Successfully connected to: " << port_name << std::endl;
            sm->release_search_lock();
            return;
        } catch (const std::exception& e) {
            std::cerr << "[L1] ERROR opening the found port: " << port_name
                      << " - " << e.what() << std::endl;
            if (sm->get_port().is_open()) {
                boost::system::error_code close_ec;
                sm->get_port().close(close_ec);
            }
        }
    }

    sm->release_search_lock();

    if (sm->is_stop_requested()) return;

    if (!timer) timer = std::make_shared<net::steady_timer>(ioc);
    std::cout << "[L1] Searching for device..." << std::endl;
    timer->expires_after(std::chrono::seconds(3));
    timer->async_wait([&ioc, sm, timer](const boost::system::error_code& ec) {
        if (ec || sm->is_stop_requested()) return;
        if (!sm->get_port().is_open())
            do_find_device(sm, ioc, timer);
    });
}

// ----------------------------------------------------------------------
//  Настройка консоли (UTF-8)
// ----------------------------------------------------------------------
void setup_console() {
#ifdef _WIN32
    SetConsoleOutputCP(65001);
    SetConsoleCP(65001);
    std::setlocale(LC_ALL, "Russian");
#endif
}

// ----------------------------------------------------------------------
//  main
// ----------------------------------------------------------------------
int main() {
    try {
        setup_console();

        net::io_context ioc;
        auto work_guard = net::make_work_guard(ioc);
        auto sm = std::make_shared<SerialManager>(ioc);
        auto server = std::make_shared<CncServer>(ioc, *sm);
        server->init();   // подписка на строки порта

        do_find_device(sm, ioc);
        start_terminal_input(ioc, sm);

        boost::asio::signal_set signals(ioc, SIGINT, SIGTERM);
        signals.async_wait([&ioc, &work_guard, sm, server](const boost::system::error_code&, int) {
            std::cout << "\n[MAIN] Shutting down..." << std::endl;
            server->stop();     // останавливаем WebSocket сервер
            sm->stop();         // останавливаем работу с портом
            work_guard.reset(); // разрешаем выход из ioc.run()
        });

        std::cout << "[L2] WebSocket server running on port 8080" << std::endl;
        std::cout << "[MAIN] Press Ctrl+C to exit" << std::endl;

        ioc.run();
        std::cout << "[MAIN] Goodbye." << std::endl;
    } catch (std::exception const& e) {
        std::cerr << "FATAL ERROR: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}