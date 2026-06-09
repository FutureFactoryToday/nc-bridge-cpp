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
    net::serial_port port;
    std::string line_accumulator;
    std::mutex accum_mutex;
    char read_buf;
    bool was_open = false;
    std::atomic<bool> is_searching{false};
    std::atomic<bool> stop_flag{false};   // флаг остановки всех операций

    // Наблюдатели
    std::mutex status_observers_mutex;
    std::vector<std::function<void(bool)>> status_observers;
    std::mutex line_observers_mutex;
    std::vector<std::function<void(const std::string&)>> line_observers;

    SerialManager(net::io_context& ioc) : port(ioc) {}

    void add_status_observer(std::function<void(bool)> observer) {
        std::lock_guard<std::mutex> lock(status_observers_mutex);
        status_observers.push_back(std::move(observer));
    }

    void add_line_observer(std::function<void(const std::string&)> observer) {
        std::lock_guard<std::mutex> lock(line_observers_mutex);
        line_observers.push_back(std::move(observer));
    }

    void notify_status_change(bool is_open) {
        std::lock_guard<std::mutex> lock(status_observers_mutex);
        for (auto& obs : status_observers) obs(is_open);
    }

    void notify_line_received(const std::string& line) {
        std::lock_guard<std::mutex> lock(line_observers_mutex);
        for (auto& obs : line_observers) obs(line);
    }

    void check_and_notify_status() {
        bool current_state = port.is_open();
        if (current_state != was_open) {
            was_open = current_state;
            notify_status_change(current_state);
        }
    }

    // Остановка всех операций порта
    void stop() {
        stop_flag = true;
        boost::system::error_code ec;
        port.cancel(ec);   // отменяем все асинхронные операции
        port.close(ec);    // закрываем порт
        notify_status_change(false);
    }
};