#pragma once

#include "thread_pool.h"

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace mooncake::kvtest {

struct HttpResponse {
    int status{200};
    std::string content_type{"text/plain"};
    std::string body{"ok"};
};

using HttpHandler = std::function<HttpResponse(
    const std::string &method, const std::string &path,
    const std::string &body)>;

class HttpServer {
   public:
    HttpServer(int port, size_t connection_threads, HttpHandler handler);
    ~HttpServer();

    bool Start(std::string *error);
    void Stop();

   private:
    void AcceptLoop();
    void HandleConnection(std::shared_ptr<asio::ip::tcp::socket> socket);

    int port_;
    HttpHandler handler_;
    asio::io_context io_;
    std::unique_ptr<asio::ip::tcp::acceptor> acceptor_;
    ThreadPool connection_pool_;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};
};

bool HttpPost(const std::string &url, const std::string &path,
              const std::string &body, std::string *error);

}  // namespace mooncake::kvtest
