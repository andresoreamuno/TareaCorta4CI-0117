//
// Created by Sleyter Angulo on 9/17/26.
//

#include "../includes/net_util.h"
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <semaphore.h>

#define _POSIX_C_SOURCE 200809L

#define DEFAULT_PORT 8080
#define LISTEN_BACKLOG 64
#define CAPACIDAD_COLA 2//Capacidad de la cola
#define DEFAULT_CONSUMIDORES 4//Cantidad de consumidores por defecto

static volatile sig_atomic_t g_running = 1;

static unsigned long g_requests_served = 0;

//static pthread_mutex_t g_served_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t g_served_sem;

typedef struct {
    int file_descriptor;
    unsigned long connection_id;
} connection_t;

typedef struct {
    connection_t conexiones[CAPACIDAD_COLA];
    int frente;
    int atras;
    sem_t libres;
    sem_t ocupados;
    sem_t bloqueado;
} cola_connection_t;

static cola_connection_t g_cola = {
    .frente = 0,
    .atras = 0
};

static void encolar(cola_connection_t *cola, connection_t conn)
{
    //bloquea el mutex
    //pthread_mutex_lock(&cola->mutex);
    while (sem_wait(&cola->libres) && errno == EINTR);
    while (sem_wait(&cola->bloqueado) && errno == EINTR);

    //Espera no activa si la cola esta llena
    //while (cola->cantidad == CAPACIDAD_COLA)
    //    pthread_cond_wait(&cola->no_lleno, &cola->mutex);
    

    //inserta conexion y actualiza variables cola
    cola->conexiones[cola->atras] = conn;
    cola->atras = (cola->atras + 1) % CAPACIDAD_COLA;
    //cola->cantidad++;

    //Avisa que hay cola
    //pthread_cond_signal(&cola->no_vacio);
    sem_post(&cola->bloqueado);
    sem_post(&cola->ocupados);
    
    //libera el mutex
    //pthread_mutex_unlock(&cola->mutex);
    

}

static int desencolar(cola_connection_t *cola, connection_t *conn)
{
    
    while (sem_wait(&cola->ocupados) && errno == EINTR);

    //bloquea el mutex
    //pthread_mutex_lock(&cola->mutex);
    while (sem_wait(&cola->bloqueado) && errno == EINTR);

    //while (cola->cantidad==0 && g_running)
    //{
    //    pthread_cond_wait(&cola->no_vacio, &cola->mutex);
    //}
    

    //Si no hay items en cola libera mutex y retorna 0 (programa cerrando)
    //if (cola->cantidad == 0){
    //    pthread_mutex_unlock(&cola->mutex);
    //    return 0;
    //}

    //Saca conexion y actualiza variables cola
    *conn = cola->conexiones[cola->frente];
    cola->frente = (cola->frente + 1) % CAPACIDAD_COLA;
    //cola->cantidad--;

    //Avisa que hay campo
    //pthread_cond_signal(&cola->no_lleno);
    sem_post(&cola->bloqueado);
    sem_post(&cola->libres);

    //libera mutex
    //pthread_mutex_unlock(&cola->mutex);
    return 1;
}

static void on_sigint(int signum)
{
    (void)signum;
    g_running = 0;
}

static int install_signal_handlers(void)
{
    struct sigaction sa;

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;

    if (sigaction(SIGINT, &sa, NULL) < 0)
    {
        perror("sigaction");
        return -1;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;

    if (sigaction(SIGPIPE, &sa, NULL) < 0)
    {
        perror("sigaction");
        return -1;
    }

    return 0;
}

static void *handle_connection(void *arg)
{
    connection_t *conn = arg;

    //printf("[Handling connection %lu] accepted\n", conn->connection_id);
    //fflush(stdout);

    if (nu_drain_request(conn->file_descriptor) > 0) //Si lee peticion ok
    {
        (void)nu_send_response(conn->file_descriptor, conn->connection_id);
    }
    
    //mutex para evitar condicion de carrera al actualizar solicitudes atendidas
    while (sem_wait(&g_served_sem) && errno == EINTR);
    //pthread_mutex_lock(&g_served_mutex);
    g_requests_served++;
    //pthread_mutex_unlock(&g_served_mutex);
    sem_post(&g_served_sem);

    //unsigned long current = g_requests_served;
    //sched_yield();
    //g_requests_served = current + 1;

    if (close(conn->file_descriptor) < 0)
        perror("close(file_descriptor)");

    //free(conn);
    return NULL;

}

static unsigned short parse_port(int argc, char **argv)
{
    if (argc < 2)
    {
        return DEFAULT_PORT;
    }

    char *end = NULL;
    errno = 0;
    long value = strtol(argv[1], &end, 10);

    if (errno != 0 || end == argv[1] || *end != '\0' ||
        value <= 0 || value > 65535) {
        fprintf(stderr, "invalid port '%s', using %d\n", argv[1], DEFAULT_PORT);
        return DEFAULT_PORT;
        }

    return (unsigned short)value;
}

static long parse_consumidores(int argc, char **argv)
{
    if (argc < 3)
    {
        return DEFAULT_CONSUMIDORES;
    }

    char *end = NULL;
    errno = 0;
    long value = strtol(argv[2], &end, 10);

    if (errno != 0 || end == argv[2] || *end != '\0' || value <= 0) {
        fprintf(stderr, "cantidad de consumidores invalida '%s', usando %d\n", argv[2], DEFAULT_CONSUMIDORES);
        return DEFAULT_CONSUMIDORES;
        }

    return value;

}

static void *consumidor(void *arg)
{
    (void)arg;
    connection_t conn;

    
    while(desencolar(&g_cola, &conn)){
        if (conn.file_descriptor == -1)
            break;
        handle_connection(&conn);
    }
        

    return NULL;
}

int main(int argc, char **argv)
{
    if (install_signal_handlers() < 0)
    {
        return EXIT_FAILURE;
    }
    unsigned short port = parse_port(argc, argv);
    long cant_consumidores = parse_consumidores(argc, argv);

    int listen_file_descriptor = nu_listen(port, LISTEN_BACKLOG);

    if (listen_file_descriptor < 0)
    {
        return EXIT_FAILURE;
    }

    //Inicializa semaforos
    sem_init(&g_cola.libres,0,CAPACIDAD_COLA);
    sem_init(&g_cola.ocupados,0,0);
    sem_init(&g_cola.bloqueado,0,1);

    //Inicializa hilos consumidores
    pthread_t *consumidores = calloc((size_t)cant_consumidores, sizeof *consumidores);

    if (consumidores == NULL){
        fprintf(stderr, "out of memory\n");
        close(listen_file_descriptor);
        return EXIT_FAILURE;
    }

    for (long i = 0; i < cant_consumidores; i++){
       int pthread_created = pthread_create(&consumidores[i], NULL, consumidor, NULL); 

       if (pthread_created != 0)
        {
            fprintf(stderr, "pthread_create failed %s\n", strerror(pthread_created));
            return EXIT_FAILURE;
        }
    }

    printf("listening on port %u — Ctrl-C to stop\n", port);
    fflush(stdout);


    //Productor
    unsigned long accepted = 0;

    while (g_running)
    {
        int client_file_descriptor = accept(listen_file_descriptor, NULL, NULL);
        if (client_file_descriptor < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            perror("accept");
            break;
        }
        
        connection_t conn;

        conn.file_descriptor = client_file_descriptor;
        conn.connection_id = ++accepted;
        encolar(&g_cola, conn);        
    }

    if (close(listen_file_descriptor))
    {
        perror("close(listen_file_descriptor)");
    }

    //marca bandera apagado y avisa a posibles hilos dormidos para que terminen
    g_running = 0;
    //pthread_mutex_lock(&g_cola.mutex);
    //pthread_cond_broadcast(&g_cola.no_vacio);
    //pthread_mutex_unlock(&g_cola.mutex);

    //Encola "pildora" para sacar a posibles hilos aun en espera
    for (long i = 0; i < cant_consumidores; i++){
        connection_t final = { .file_descriptor = -1,
                               .connection_id = 0
                             };
        encolar(&g_cola, final);
    }

    //hace el join de los hilos antes de cerrar
    for (long i = 0; i < cant_consumidores; i++){
        int rc = pthread_join(consumidores[i], NULL);
        if (rc != 0)
            fprintf(stderr, "pthread_join: %s\n", strerror(rc));
    }

    //libera recursos
    free(consumidores);

    //pthread_mutex_destroy(&g_cola.mutex);
    //pthread_cond_destroy(&g_cola.no_lleno);
    //pthread_cond_destroy(&g_cola.no_vacio);

    sem_destroy(&g_cola.bloqueado);
    sem_destroy(&g_cola.libres);
    sem_destroy(&g_cola.ocupados);

    printf("\naccepted: %lu\n", accepted);
    printf("served:   %lu\n", g_requests_served);
    printf("lost:     %ld\n", (long)accepted - (long)g_requests_served);

    return EXIT_SUCCESS;
}