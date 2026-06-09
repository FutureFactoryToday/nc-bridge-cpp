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
    SerialManager(net::io_context& ioc) 
        : port(ioc) {}
    // Публичные методы доступа к внутренним данным
    net::serial_port& get_port();
    std::mutex& get_accum_mutex();
    std::string& get_line_accumulator();
    char& get_read_buf();
    bool get_was_open() const;
    void set_was_open(bool val);
    bool acquire_search_lock();
    void release_search_lock();
    bool is_stop_requested() const;
    void set_stop_requested();
    // Методы для подписки на события
    void add_status_observer(std::function<void(bool)> observer);
    void add_line_observer(std::function<void(const std::string&)> observer);
    // Уведомления
    void notify_status_change(bool is_open);
    void notify_line_received(const std::string& line);
    // Проверка и уведомление о смене статуса порта
    void check_and_notify_status();
    // Остановка всех операций порта
    void stop();
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

public:
    CncSession(tcp::socket socket, SerialManager& sm)
        : ws_(std::move(socket)), sm_(sm) {}
    ~CncSession();
    void init();
    void start();
    void deliver(const std::string& message);
    // Принудительное закрытие сессии (для остановки)
    void stop();
private:
    websocket::stream<tcp::socket> ws_;
    SerialManager& sm_;
    beast::flat_buffer ws_buffer_;
    std::deque<std::string> write_queue_;
    std::shared_ptr<net::steady_timer> status_timer_;
    std::atomic<bool> status_timer_active_{false};

    void send_status(bool port_open);
    void safe_send(std::string msg);
    void do_write();
    void start_periodic_status();
    void schedule_status_timer();
    void stop_periodic_status();
    void do_read_ws();
};

class CncServer : public std::enable_shared_from_this<CncServer> {

public:
    CncServer(net::io_context& ioc, SerialManager& sm);
    void init();
    void broadcast(const std::string& message);
    // Остановка сервера: закрываем acceptor и все сессии
    void stop();

private:
    tcp::acceptor acceptor_;
    SerialManager& sm_;
    std::vector<std::weak_ptr<CncSession>> sessions_;
    std::mutex sessions_mutex_;
    std::atomic<bool> stop_flag_{false};

    void do_accept();
};