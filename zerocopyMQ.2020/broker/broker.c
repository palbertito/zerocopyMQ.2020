//Nombre autor: Alberto Doncel Aparicio
//Matrícula: y160364
#include <stdio.h>
#include "comun.h"
#include "diccionario.h"
#include "cola.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <netdb.h>
#include <netinet/in.h>
#include <limits.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/uio.h>

#define MAX 256

struct msg
{ 
   void *contenido;
   int tam;  
};

int main(int argc, char *argv[]) {
	int s, s_conec, res, error, tamanyo_cola;
	char linea[MAXQUEUENAME];
	unsigned int tam_dir;
	struct sockaddr_in dir, dir_cliente;
	int opcion=1;
	char * token,* ptr,* nombrecola;
	struct iovec iovm[2];
	struct diccionario *dicionarioColas=dic_create(); //diccionario de colas
	char str[16];
	FILE *desc_soc;
	void *p;//,*q;
	struct msg *mensaje;
	struct cola *queue;

	//creamos socket
	if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		perror("error creando socket");
		return 1;
	}
	/* Para reutilizar puerto inmediatamente */
        if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion))<0){
                perror("error en setsockopt");
                return 1;
        }
	
	//nos conectamos a un puerto y hacemos bind para poder recibir a los clientes
	dir.sin_addr.s_addr=INADDR_ANY;
	dir.sin_port=htons(atoi(argv[1]));
	printf("puerto conectado: %d\n",dir.sin_port);
	dir.sin_family=PF_INET;
	if (bind(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
		perror("error en bind");
		close(s);
		return 1;
	}

	//ponemos al broker a escuchar
	if (listen(s, 0) < 0) {
		perror("error en listen");
		close(s);
		return 1;
	}
	while (1) {
		tam_dir=sizeof(dir_cliente);
		//aceptamos conexion del cliente
		if ((s_conec=accept(s, (struct sockaddr *)&dir_cliente, &tam_dir))<0){
			printf("error en accept");
			close(s);
			return -1;
		}

		//extraemos la cabecera del mensaje
		desc_soc=fdopen(s_conec, "r");
		setbuf(desc_soc, NULL);
		fgets(linea, MAXQUEUENAME, desc_soc);
		//sustituimos el \n por un \0 para poder trabajar con la cabecera como un string
		if( (ptr = strchr(linea, '\n')) != NULL)*ptr = '\0';
		printf("cabecera: %s\n",linea);

		//discernimos entre si el mensaje es un create, un destroy, un put o un get
		//la forma que uso para sacar la informacion de los distintos campos de cabecera es strtok
		//si se quiere buscar mas informacion: man strtok
		token = strtok(linea, "&");
		fprintf(stdout,"accion: %s \n",linea);
			//caso create
			if(strcmp(token, "C")==0){
				//sacamos el tamanyo de la cola
				tamanyo_cola=atoi(strtok(NULL, "&"));
				fprintf(stdout,"tamanyo cola: %d\n",tamanyo_cola);
				//guardamos el nombre de la cola en el heap como un string
				nombrecola=malloc(tamanyo_cola+1);
				fread(nombrecola,1,tamanyo_cola,desc_soc);
				nombrecola[tamanyo_cola]='\0';
				printf("nombre cola:%s\n",nombrecola);
				//creamos una cola asociado al nombre de la cola y la guardamos en el diccionario de colas
				if((res=dic_put(dicionarioColas,nombrecola,cola_create()))==0){
					// si todo va bien generamos mensaje de ok para cliente
					iovm[0].iov_base = "OK"; iovm[0].iov_len = strlen("OK")+1;
				} else if(res<0) {
					// si va mal enviamos mensaje de fail para el cliente
					iovm[0].iov_base = "FAIL"; iovm[0].iov_len = strlen("FAIL")+1;					
				}
				//enviamos mensaje al cliente para que sepa si la operacion ha ido bien o mal
				writev(s_conec, iovm, 1);	
			}
			//caso destroy
			else if(strcmp(token, "D")==0){
				//sacamos el tamanyo de la cola				
				tamanyo_cola=atoi(strtok(NULL, "&"));
				fprintf(stdout,"tamanyo cola: %d\n",tamanyo_cola);
				nombrecola=strtok(NULL,"&");
				//destruimos la cola después de buscarla en el diccionario
				if ((res=cola_destroy(dic_get(dicionarioColas, nombrecola, &error),NULL))==0){
					iovm[0].iov_base = "OK"; iovm[0].iov_len = strlen("OK")+1;
				//destruimos la entrada del diccionario correpondiente a la cola destruida
					if((res=dic_remove_entry(dicionarioColas,nombrecola,NULL))==0){
						iovm[0].iov_base = "OK"; iovm[0].iov_len = strlen("OK")+1;
					} else if(res<0) {
						iovm[0].iov_base = "FAIL"; iovm[0].iov_len = strlen("FAIL")+1;
					}	
				} else if (res<0){
					iovm[0].iov_base = "FAIL"; iovm[0].iov_len = strlen("FAIL")+1;
				}	
				//enviamos mensaje de estado al cliente
				writev(s_conec, iovm, 1);		
			}
			//caso put
			else if(strcmp(token, "P")==0){
				//creamos una instancia de un struct mensaje para que sea rellenada con el mensaje recibido
				//se almacena en el heap
				mensaje=malloc(sizeof (struct msg));
				mensaje->tam=atoi(strtok(NULL, "&"));//tamaño
				fprintf(stdout,"tamanyo msg: %d\n",mensaje->tam);
				tamanyo_cola=atoi(strtok(NULL, "&"));
				fprintf(stdout,"tamanyo cola: %d\n",tamanyo_cola);
				//buscamos cola en la que introducir mensaje
				queue=dic_get(dicionarioColas, strtok(NULL, "&"), &error);
				if(error==0){
					printf("cola :%p\n",queue);
					p=malloc(mensaje->tam);
					//guardamos el mensaje que recibimos fuera de la cabecera en el heap
					recv(s_conec,p,mensaje->tam,MSG_WAITALL);
					mensaje->contenido=p;
					fprintf(stdout, "contenido : %s tam:%d\n",mensaje->contenido,mensaje->tam);	
					//introducimos mensaje en la cola
					if(cola_push_back(queue, mensaje)==0){
						iovm[0].iov_base = "OK"; iovm[0].iov_len = strlen("OK")+1;
					} else {
							iovm[0].iov_base = "FAIL"; iovm[0].iov_len = strlen("FAIL")+1;
					}

				} else {
						iovm[0].iov_base = "FAIL"; iovm[0].iov_len = strlen("FAIL")+1;
				}
				//enviamos mensaje de estado al cliente						
				writev(s_conec, iovm, 1);
			}
			//caso get
			else if(strcmp(token, "G")==0){
				//creamos struct mensaje para almacenar el que vamos a extraer de la cola
				mensaje=malloc(sizeof (struct msg));
				//buscamos la cola pedida
				queue=dic_get(dicionarioColas, strtok(NULL, "&"), &error);
				if (error == 0 ){
					fprintf(stdout,"queue: %p",queue);
					//extraemos el mensaje mas antiguo
					mensaje=cola_pop_front(queue, &error);
						if (error == 0){
							//en caso sin errores metemos el tamanyo del mensaje y el contenido en un iov
							printf("contenido: %s, tam: %d\n", mensaje->contenido, mensaje->tam);
							sprintf(str, "%d", mensaje->tam);
							iovm[0].iov_base=strcat(str,"\n"); iovm[0].iov_len=strlen(str);	
							iovm[1].iov_base=(char *)mensaje->contenido; iovm[1].iov_len = (mensaje->tam);
						} else if(error < 0) {
							//caso de error, significa que es una cola vacia, enviamos esta informacion al cliente
							sprintf(str, "%d", 0);
							iovm[0].iov_base=strcat(str,"\n"); iovm[0].iov_len=strlen(str);	
						}
				} else if (error < 0) {
					//caso de error que significa que la cola no existe, enviamos esta informacion al cliente
					sprintf(str, "%d", -1);
					iovm[0].iov_base=strcat(str,"\n"); iovm[0].iov_len=strlen(str);	
				}
				//enviamos informacion al cliente
				writev(s_conec, iovm, 2);				
				free(mensaje);
			}
		
		close(s_conec);
		}


		close(s);
		return 0;
	}

	