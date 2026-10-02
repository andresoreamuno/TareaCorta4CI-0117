## Tarea Corta 04 – Semaphores Server 

(Implementado sobre Tarea Corta 03 Producer-Consumer)

### Instrucciones

- Instalar las dependencias requeridas (`sudo apt install build-essential`).

- Para compilar el proyecto:

```bash
make
```

- Para ejecutar el servidor:

```bash
./bin/server_producer-consumer <puerto> <consumidores>
```

Ejemplo:

```bash
./bin/server_producer-consumer 8080 4
```

El servidor se detiene con Ctrl+C.

- Para ejecutar el cliente:

```bash
./bin/load_client <ip> <puerto> <hilos> <peticiones-por-hilo>
```

Ejemplo:

```bash
./bin/load_client 127.0.0.1 8080 10 3000
```

- Restringir los cores usados:

```bash
taskset -c 0 ./bin/server_producer-consumer 8080 1     # 1 core
taskset -c 0,1 ./bin/server_producer-consumer 8080 1   # 2 cores
```

- Medir el tiempo con el comando `time`:

```bash
time ./bin/load_client 127.0.0.1 8080 10 3000
```
