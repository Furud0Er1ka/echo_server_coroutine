#include <algorithm>
#include <bits/stdc++.h>
#include <coroutine>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <netinet/in.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <tuple>
#include <unistd.h>

namespace IO_Tasks {
static sockaddr_in serv_addr, client_addr;
static socklen_t client_len = sizeof( client_addr );
static int sock_listen_fd;

constexpr int MAX_CONNECTIONS = 4096;
constexpr int MAX_MSG_SIZE = 2048;
constexpr int gid = 773;
static char io_bufs[ MAX_CONNECTIONS ][ MAX_MSG_SIZE ] = { 0 };
enum connection_stats {
    ACCEPT,
    READ,
    WRITE,
    PROV_BUF,
};
struct connection_info {
    uint32_t fd;
    uint16_t type;
    uint16_t bid;
};
void add_accept( io_uring *ring, int fd, sockaddr *client_addr,
                 socklen_t *client_len, unsigned flags ) {
    auto sqe = io_uring_get_sqe( ring );
    io_uring_prep_accept( sqe, fd, client_addr, client_len, 0 );
    io_uring_sqe_set_flags( sqe, flags );

    connection_info conn_tmp = {
        .fd = (uint32_t) fd,
        .type = (uint16_t) connection_stats::ACCEPT,
    };
    memcpy( &sqe->user_data, &conn_tmp, sizeof( conn_tmp ) );
}

void add_socket_read( io_uring *ring, int fd, unsigned gid, size_t msg_size,
                      unsigned flags ) {
    auto sqe = io_uring_get_sqe( ring );
    io_uring_prep_recv( sqe, fd, nullptr, msg_size, 0 );
    io_uring_sqe_set_flags( sqe, flags );
    sqe->buf_group = gid;

    {
        connection_info conn_tmp = {
            .fd = (uint32_t) fd,
            .type = (uint16_t) connection_stats::READ,
        };
        memcpy( &sqe->user_data, &conn_tmp, sizeof( conn_tmp ) );
    }
}

void add_socket_write( io_uring *ring, int fd, unsigned bid, size_t msg_size,
                       unsigned flags ) {
    auto sqe = io_uring_get_sqe( ring );
    io_uring_prep_send( sqe, fd, &io_bufs[ bid ], msg_size, 0 );
    io_uring_sqe_set_flags( sqe, flags );

    {
        connection_info conn_tmp = {
            .fd = (uint32_t) fd,
            .type = (uint16_t) connection_stats::WRITE,
            .bid = (uint16_t) bid,
        };
        memcpy( &sqe->user_data, &conn_tmp, sizeof( conn_tmp ) );
    }
} // sqe to buffer

void add_provide_buf( io_uring *ring, uint16_t bid, unsigned gid ) {
    auto sqe = io_uring_get_sqe( ring );
    io_uring_prep_provide_buffers( sqe, io_bufs[ bid ], MAX_MSG_SIZE, 1, gid,
                                   bid );

    {
        connection_info conn_tmp = {
            .fd = 0,
            .type = (uint16_t) connection_stats::PROV_BUF,
        };
        memcpy( &sqe->user_data, &conn_tmp, sizeof( conn_tmp ) );
    }
}
} // namespace IO_Tasks
using namespace IO_Tasks;


void run_io_uring() {
    io_uring ring;
    io_uring_sqe *sqe;
    io_uring_cqe *cqe;
    io_uring_params params;
    memset( &params, 0, sizeof( params ) );

    if( io_uring_queue_init_params( 2048, &ring, &params ) < 0 ) {
        perror( "io_uring_init_failed...\n" );
        exit( 1 );
    } // 2048 : queue depth
    sqe = io_uring_get_sqe( &ring );
    io_uring_prep_provide_buffers( sqe, io_bufs, MAX_MSG_SIZE, MAX_CONNECTIONS,
                                   gid, 0 );
    io_uring_submit( &ring );
    io_uring_wait_cqe( &ring, &cqe );

    if( cqe->res < 0 ) {
        fprintf( stdout, "cqe->res\n" );
        exit( 1 );
    }
    io_uring_cqe_seen( &ring, cqe );
    add_accept( &ring, sock_listen_fd, (sockaddr *) &client_addr, &client_len,
                0 );

    while( 1 ) {
        io_uring_submit_and_wait( &ring, 1 );
        io_uring_cqe *cqe;
        int head, count = 0;
        io_uring_for_each_cqe( &ring, head, cqe ) {
            ++count;
            connection_info conn_tmp;
            memcpy( &conn_tmp, &cqe->user_data, sizeof( conn_tmp ) );
            auto type = conn_tmp.type;
            if( cqe->res == -ENOBUFS ) {
                fprintf( stdout, "bufs in automatic buffer selection "
                                 "empty\n" );
                fflush( stdout );
                exit( 1 );
            } else if( type == connection_stats::PROV_BUF ) {
                if( cqe->res < 0 ) {
                    printf( "cqe->res = %d\n", cqe->res );
                    exit( 1 );
                }
            } else if( type == connection_stats::ACCEPT ) {
                int sock_conn_fd = cqe->res;
                if( sock_conn_fd >= 0 ) {
                    add_socket_read( &ring, sock_conn_fd, gid, MAX_MSG_SIZE,
                                     IOSQE_BUFFER_SELECT );
                    add_accept( &ring, sock_listen_fd,
                                (sockaddr *) &client_addr, &client_len, 0 );
                }
                add_accept( &ring, sock_listen_fd, (sockaddr *) &client_addr,
                            &client_len, 0 );
            } else if( type == connection_stats::READ ) {
                int bytes_read = cqe->res;
                int bid = cqe->flags >> 16;
                if( cqe->res <= 0 ) {
                    add_provide_buf( &ring, bid, gid );
                    shutdown( conn_tmp.fd, SHUT_RDWR );
                } else {
                    add_socket_write( &ring, conn_tmp.fd, bid, bytes_read, 0 );
                }
            } else if( type == connection_stats::WRITE ) {
                add_provide_buf( &ring, conn_tmp.bid, gid );
                add_socket_read( &ring, conn_tmp.fd, gid, MAX_MSG_SIZE,
                                 IOSQE_BUFFER_SELECT );
            }
        }
        io_uring_cq_advance( &ring, count );
    }
}

struct connection_task {
    struct promise_type {
        using Handle = std::coroutine_handle< promise_type >;
        connection_task get_return_object() {
            return connection_task { Handle::from_promise( *this ) };
        }
        std::suspend_always initial_suspend() noexcept {
            return {};
        }
        std::suspend_never final_suspend() noexcept {
            return {};
        }
        void return_void() noexcept { }
        void unhandled_exception() noexcept { }
        io_uring *ring;
        connection_info conn_info;
        size_t res;
    };
    promise_type::Handle handler;
    explicit connection_task( promise_type::Handle handler )
        : handler( handler ) { }
    void destroy() {
        handler.destroy();
    }
    connection_task( const connection_task & ) = delete;
    connection_task &operator=( const connection_task & ) = delete;
    connection_task( connection_task &&t ) noexcept : handler( t.handler ) {
        t.handler = {};
    }
    connection_task &operator=( connection_task &&t ) noexcept {
        if( this == &t ) return *this;
        if( handler ) handler.destroy();
        handler = t.handler;
        t.handler = {};
        return *this;
    }
};

auto read( size_t message_size, unsigned flags ) {
    struct awaitable {
        bool await_ready() {
            return false;
        }
        void await_suspend(
            std::coroutine_handle< connection_task::promise_type > h ) {
            auto &&p = h.promise();
            auto sqe = io_uring_get_sqe( p.ring );
            io_uring_prep_recv( sqe, p.conn_info.fd, nullptr, message_size, 0 );
            io_uring_sqe_set_flags( sqe, flags );
            sqe->buf_group = gid;
            p.conn_info.type = READ;
            memcpy( &sqe->user_data, &p.conn_info, sizeof( connection_info ) );
            this->p = &p;
        }
        size_t await_resume() {
            return p->res;
        }
        size_t message_size;
        unsigned flags;
        connection_task::promise_type *p = nullptr;
    };
    return awaitable { message_size, flags };
}
auto write( size_t message_size, unsigned flags ) {
    struct awaitable {
        bool await_ready() {
            return false;
        }
        void await_suspend(
            std::coroutine_handle< connection_task::promise_type > h ) {
            auto &&p = h.promise();
            struct io_uring_sqe *sqe = io_uring_get_sqe( p.ring );
            io_uring_prep_send( sqe, p.conn_info.fd,
                                &io_bufs[ p.conn_info.bid ], message_size, 0 );
            io_uring_sqe_set_flags( sqe, flags );
            p.conn_info.type = WRITE;
            memcpy( &sqe->user_data, &p.conn_info, sizeof( connection_info ) );
        }
        size_t await_resume() {
            return 0;
        }
        size_t message_size;
        unsigned flags;
    };
    return awaitable { message_size, flags };
}


auto add_buffer() {
    struct awaitable {
        bool await_ready() {
            return false;
        }
        void await_suspend(
            std::coroutine_handle< connection_task::promise_type > h ) {
            auto &&p = h.promise();
            struct io_uring_sqe *sqe = io_uring_get_sqe( p.ring );
            io_uring_prep_provide_buffers( sqe, io_bufs[ p.conn_info.bid ],
                                           MAX_MSG_SIZE, 1, gid,
                                           p.conn_info.bid );
            p.conn_info.type = PROV_BUF;
            memcpy( &sqe->user_data, &p.conn_info, sizeof( connection_info ) );
            h.resume();
        }
        size_t await_resume() {
            return 0;
        }
    };
    return awaitable {};
}
std::map< int, connection_task > connections;

connection_task handle_echo( int fd ) {
    while( 1 ) {
        size_t size_r = co_await read( MAX_MSG_SIZE, IOSQE_BUFFER_SELECT );
        if( size_r <= 0 ) {
            co_await add_buffer();
            shutdown( fd, SHUT_RDWR );
            connections.erase( fd );
            co_return;
        }
        co_await write( size_r, 0 );
        co_await add_buffer();
    }
}

void run_io_couring() {
    io_uring ring;
    io_uring_sqe *sqe;
    io_uring_cqe *cqe;
    io_uring_params params;
    memset( &params, 0, sizeof( params ) );

    if( io_uring_queue_init_params( 2048, &ring, &params ) < 0 ) {
        perror( "io_uring_init_failed...\n" );
        exit( 1 );
    } // 2048 : queue depth
    sqe = io_uring_get_sqe( &ring );
    io_uring_prep_provide_buffers( sqe, io_bufs, MAX_MSG_SIZE, MAX_CONNECTIONS,
                                   gid, 0 );
    io_uring_submit( &ring );
    io_uring_wait_cqe( &ring, &cqe );

    if( cqe->res < 0 ) {
        fprintf( stdout, "cqe->res\n" );
        exit( 1 );
    }
    io_uring_cqe_seen( &ring, cqe );
    add_accept( &ring, sock_listen_fd, (sockaddr *) &client_addr, &client_len,
                0 );

    while( 1 ) {
        io_uring_submit_and_wait( &ring, 1 );
        io_uring_cqe *cqe;
        int head, count = 0;
        io_uring_for_each_cqe( &ring, head, cqe ) {
            ++count;
            connection_info conn_tmp;
            memcpy( &conn_tmp, &cqe->user_data, sizeof( conn_tmp ) );
            auto type = conn_tmp.type;
            if( cqe->res == -ENOBUFS ) {
                fprintf( stdout, "bufs in automatic buffer selection "
                                 "empty\n" );
                fflush( stdout );
                exit( 1 );
            } else if( type == connection_stats::PROV_BUF ) {
                if( cqe->res < 0 ) {
                    printf( "cqe->res = %d\n", cqe->res );
                    exit( 1 );
                }
            } else if( type == connection_stats::ACCEPT ) {
                int sock_conn_fd = cqe->res;
                if( sock_conn_fd >= 0 ) {
                    connections.emplace( sock_conn_fd,
                                         handle_echo( sock_conn_fd ) );
                    auto &&h = connections.at( sock_conn_fd ).handler;
                    auto &&p = h.promise();
                    p.conn_info.fd = sock_conn_fd;
                    p.ring = &ring;
                    h.resume();
                }
                add_accept( &ring, sock_listen_fd, (sockaddr *) &client_addr,
                            &client_len, 0 );
            } else if( type == connection_stats::READ ) {
                auto &&h = connections.at( conn_tmp.fd ).handler;
                auto &&p = h.promise();
                p.conn_info.bid = cqe->flags >> 16;
                p.res = cqe->res;
                h.resume();
            } else if( type == connection_stats::WRITE ) {
                auto &&h = connections.at( conn_tmp.fd ).handler;
                h.resume();
            }
        }
        io_uring_cq_advance( &ring, count );
    }
}

int main( int argc, char **argv ) {
    int portnum = strtol( argv[ 1 ], nullptr, 10 );

    sock_listen_fd = socket( AF_INET, SOCK_STREAM, 0 );
    {
        int _ = 1;
        setsockopt( sock_listen_fd, SOL_SOCKET, SO_REUSEADDR, &_, sizeof( _ ) );
    } // address reusing
    memset( &serv_addr, 0, sizeof( serv_addr ) );
    serv_addr = { .sin_family = AF_INET,
                  .sin_port = htons( portnum ),
                  .sin_addr = { .s_addr = INADDR_ANY } };
    if( bind( sock_listen_fd, (sockaddr *) &serv_addr, sizeof( serv_addr ) ) <
        0 ) {
        perror( "bind" );
        exit( 1 );
    }
    if( listen( sock_listen_fd, 512 ) < 0 ) {
        perror( "listen" );
        exit( 1 );
    }
    fprintf( stdout, "Server established on port %d\n", portnum );
    // run_io_uring();
    run_io_couring();

    return 0;
}
