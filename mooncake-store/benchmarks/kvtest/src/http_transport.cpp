#include "http_transport.h"

#include <asio/connect.hpp>
#include <asio/read.hpp>
#include <asio/read_until.hpp>
#include <asio/steady_timer.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>
#include <sys/socket.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <istream>
#include <ostream>
#include <sstream>

namespace mooncake::kvtest {
namespace {

struct Endpoint {
    std::string host;
    std::string port;
};

bool ParseUrl(const std::string &url, Endpoint *endpoint, std::string *error) {
    std::string value = url;
    constexpr const char *prefix = "http://";
    if (value.rfind(prefix, 0) == 0)
        value.erase(0, std::char_traits<char>::length(prefix));
    const auto slash = value.find('/');
    if (slash != std::string::npos) value.resize(slash);
    const auto colon = value.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == value.size()) {
        if (error) *error = "invalid HTTP peer URL: " + url;
        return false;
    }
    endpoint->host = value.substr(0, colon);
    endpoint->port = value.substr(colon + 1);
    return true;
}

std::string Trim(std::string value) {
    while (!value.empty() &&
           std::isspace(static_cast<unsigned char>(value.back())))
        value.pop_back();
    size_t start = 0;
    while (start < value.size() &&
           std::isspace(static_cast<unsigned char>(value[start])))
        ++start;
    return value.substr(start);
}

}  // namespace

HttpServer::HttpServer(int port, size_t connection_threads,
                       HttpHandler handler)
    : port_(port),
      handler_(std::move(handler)),
      connection_pool_(std::max<size_t>(1, connection_threads), 4096) {}

HttpServer::~HttpServer() { Stop(); }

bool HttpServer::Start(std::string *error) {
    try {
        acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(io_);
        acceptor_->open(asio::ip::tcp::v4());
        acceptor_->set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_->bind(
            asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port_));
        acceptor_->listen();
        running_.store(true, std::memory_order_release);
        accept_thread_ = std::thread([this] { AcceptLoop(); });
        return true;
    } catch (const std::exception &e) {
        if (error) *error = e.what();
        return false;
    }
}

void HttpServer::Stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;
    if (acceptor_) {
        asio::error_code ec;
        acceptor_->cancel(ec);
        acceptor_->close(ec);
    }
    io_.stop();
    if (accept_thread_.joinable()) accept_thread_.join();
    connection_pool_.Stop();
}

void HttpServer::AcceptLoop() {
    while (running_.load(std::memory_order_acquire)) {
        auto socket = std::make_shared<asio::ip::tcp::socket>(io_);
        asio::error_code ec;
        acceptor_->accept(*socket, ec);
        if (ec) {
            if (!running_.load(std::memory_order_acquire)) return;
            continue;
        }
        socket->set_option(asio::ip::tcp::no_delay(true), ec);
        if (!connection_pool_.Submit(
                [this, socket] { HandleConnection(socket); })) {
            socket->close(ec);
        }
    }
}

void HttpServer::HandleConnection(
    std::shared_ptr<asio::ip::tcp::socket> socket) {
    try {
        asio::streambuf headers;
        asio::read_until(*socket, headers, "\r\n\r\n");
        std::istream input(&headers);
        std::string request_line;
        std::getline(input, request_line);
        request_line = Trim(request_line);
        std::istringstream request_stream(request_line);
        std::string method;
        std::string path;
        std::string version;
        request_stream >> method >> path >> version;

        size_t content_length = 0;
        std::string line;
        while (std::getline(input, line) && line != "\r") {
            const auto colon = line.find(':');
            if (colon == std::string::npos) continue;
            std::string name = line.substr(0, colon);
            std::transform(name.begin(), name.end(), name.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            if (name == "content-length") {
                content_length =
                    static_cast<size_t>(std::stoull(Trim(line.substr(colon + 1))));
            }
        }

        constexpr size_t kMaxRequestBody = 4 * 1024 * 1024;
        if (content_length > kMaxRequestBody) {
            static constexpr char response[] =
                "HTTP/1.1 413 Payload Too Large\r\n"
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            asio::write(*socket, asio::buffer(response, sizeof(response) - 1));
            return;
        }

        std::string body(content_length, '\0');
        size_t copied = 0;
        while (copied < content_length && headers.size() > 0) {
            input.read(body.data() + copied,
                       static_cast<std::streamsize>(content_length - copied));
            const auto read = static_cast<size_t>(input.gcount());
            if (read == 0) break;
            copied += read;
        }
        if (copied < content_length) {
            asio::read(*socket,
                       asio::buffer(body.data() + copied,
                                    content_length - copied));
        }

        HttpResponse response;
        if (method.empty() || path.empty()) {
            response = {400, "text/plain", "bad request"};
        } else {
            response = handler_(method, path, body);
        }
        std::ostringstream output;
        output << "HTTP/1.1 " << response.status
               << (response.status == 200 ? " OK" : " ERROR") << "\r\n"
               << "Content-Type: " << response.content_type << "\r\n"
               << "Content-Length: " << response.body.size() << "\r\n"
               << "Connection: close\r\n\r\n"
               << response.body;
        asio::write(*socket, asio::buffer(output.str()));
    } catch (...) {
    }
    asio::error_code ec;
    socket->shutdown(asio::ip::tcp::socket::shutdown_both, ec);
    socket->close(ec);
}

bool HttpPost(const std::string &url, const std::string &path,
              const std::string &body, std::string *error) {
    try {
        Endpoint endpoint;
        if (!ParseUrl(url, &endpoint, error)) return false;
        asio::io_context io;
        asio::ip::tcp::resolver resolver(io);
        asio::ip::tcp::socket socket(io);
        asio::steady_timer timer(io);
        asio::error_code connect_error = asio::error::would_block;
        timer.expires_after(std::chrono::seconds(3));
        timer.async_wait([&](const asio::error_code &timer_error) {
            if (!timer_error && connect_error == asio::error::would_block) {
                asio::error_code ignored;
                socket.close(ignored);
            }
        });
        asio::async_connect(
            socket, resolver.resolve(endpoint.host, endpoint.port),
            [&](const asio::error_code &ec, const asio::ip::tcp::endpoint &) {
                connect_error = ec;
                timer.cancel();
            });
        io.run();
        if (connect_error) {
            if (error)
                *error = connect_error == asio::error::operation_aborted
                             ? "connect timeout"
                             : connect_error.message();
            return false;
        }
        socket.set_option(asio::ip::tcp::no_delay(true));
        timeval timeout{3, 0};
        setsockopt(socket.native_handle(), SOL_SOCKET, SO_SNDTIMEO, &timeout,
                   sizeof(timeout));
        setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout));
        std::ostringstream request;
        request << "POST " << path << " HTTP/1.1\r\n"
                << "Host: " << endpoint.host << "\r\n"
                << "Content-Type: application/json\r\n"
                << "Content-Length: " << body.size() << "\r\n"
                << "Connection: close\r\n\r\n"
                << body;
        asio::write(socket, asio::buffer(request.str()));
        asio::streambuf response;
        asio::read_until(socket, response, "\r\n");
        std::istream input(&response);
        std::string version;
        int status = 0;
        input >> version >> status;
        if (status != 200) {
            if (error) *error = "HTTP status " + std::to_string(status);
            return false;
        }
        return true;
    } catch (const std::exception &e) {
        if (error) *error = e.what();
        return false;
    }
}

}  // namespace mooncake::kvtest
