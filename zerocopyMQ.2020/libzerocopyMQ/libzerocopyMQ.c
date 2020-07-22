//Nombre autor: Alberto Doncel Aparicio
//Matrícula: y160364

#include <stdint.h>
#include "zerocopyMQ.h"
#include "comun.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <limits.h>
#include <fcntl.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/uio.h>

int createMQ(const char *cola) {
	int s, leido;
	struct sockaddr_in dir;
	struct hostent *host_info;
	struct iovec iov[4];
	char buf[16];
	char *tamanyo;

	//comprobamos longitud cola
	if(strlen(cola)>=MAXQUEUENAME){
		printf("el nombre de la cola excede máximo permitido\n");
		return -1;
	}

	//creamos socket
	if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		printf("error creando socket");
		return -1;
	}

	//nos conectamos con el broker sacando informacion de las variables de entorno
	host_info=gethostbyname(getenv("BROKER_HOST"));
	// 2 alternativas
	//memcpy(&dir.sin_addr.s_addr, host_info->h_addr, host_info->h_length);
	dir.sin_addr=*(struct in_addr *)host_info->h_addr;
	dir.sin_port=htons(atoi(getenv("BROKER_PORT")));	
	dir.sin_family=PF_INET;
	printf("puerto %d\n",dir.sin_port);
	if (connect(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
		perror("error en connect");
		close(s);
		return 1;
	}
	//creamos cabecera tipo : C&tamanyonombrecola&\n
	char cabecera[]="C&";
	tamanyo=malloc(16);
	sprintf(tamanyo, "%ld%s", strlen(cola),"&");

	//la introducimos en un iov
	iov[0].iov_base = (char *)cabecera; iov[0].iov_len = strlen(cabecera);
	iov[1].iov_base = tamanyo; iov[1].iov_len = strlen(tamanyo);
	iov[2].iov_base = "\n"; iov[2].iov_len = 1;

	//metemos cola fuera de la cabecera
	iov[3].iov_base = (char *)cola; iov[3].iov_len = strlen(cola);
	
	//enviamos la informacion
	writev(s, iov, 4);

	//esperamos la respuesta del broker, la recibimos y actuamos según sea esta
	while((leido=recv(s, buf, TAM,0))>0){
		printf("contenido buf: %s\n",buf);
		if(strcmp(buf,"OK")==0) {
			free(tamanyo);
			close(s);
			return 0;
		}
		else if(strcmp(buf,"FAIL")==0){
			printf("Creacion de cola fallida\n");
			free(tamanyo);
			close(s);
			return -1;
		}
		if (leido<0) {
			printf("error en read");
			free(tamanyo);
			close(s);
			return -1;
		}
	}
	free(tamanyo);
	close(s);
	return 0;
}
int destroyMQ(const char *cola){
	int s, leido;
	char buf[16];
	struct sockaddr_in dir;
	struct hostent *host_info;
	struct iovec iov[4];
	char *tamanyo;

	//comprobamos longitud cola
	if(strlen(cola)>=MAXQUEUENAME){
		printf("el nombre de la cola excede máximo permitido\n");
		return -1;
	}

	//creamos socket
	if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		printf("error creando socket");
		return -1;
	}

	//nos conectamos con el broker sacando informacion de las variables de entorno
	host_info=gethostbyname(getenv("BROKER_HOST"));
	dir.sin_addr=*(struct in_addr *)host_info->h_addr;
	dir.sin_port=htons(atoi(getenv("BROKER_PORT")));	
	dir.sin_family=PF_INET;
	printf("puerto %d\n",dir.sin_port);
	if (connect(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
		perror("error en connect");
		close(s);
		return -1;
	}

	//creamos cabecera tipo : D&tamanyonombrecola&nombrecola\n
	char cabecera[]="D";
	tamanyo=malloc(16);
	sprintf(tamanyo, "%ld%s", strlen(cola),"&");
	//la introducimos en un iov
	iov[0].iov_base = strcat(cabecera,"&"); iov[0].iov_len = strlen(cabecera);
	iov[1].iov_base = tamanyo; iov[1].iov_len = strlen(tamanyo);
	iov[2].iov_base = (char *)cola; iov[2].iov_len = strlen(cola); //introducimos '\0'
	iov[3].iov_base = "\n"; iov[3].iov_len = 1;
	
	//enviamos la informacion
	writev(s, iov, 4);

	//esperamos la respuesta del broker, la recibimos y actuamos según sea esta
	while((leido=recv(s, buf, TAM,0))>0){
		printf("contenido buf: %s\n",buf);
		if(strcmp(buf,"OK")==0) {
			close(s);
			return 0;
		}
		else if(strcmp(buf,"FAIL")==0){
			printf("Creación de cola fallida");
			close(s);
			return -1;
		}
		if (leido<0) {
			printf("error en read");
			close(s);
			return -1;
		}
	}
	free(tamanyo);
	close(s);
	return 0;
}
int put(const char *cola, const void *mensaje, uint32_t tam) {
	int s, leido;
	struct sockaddr_in dir;
	struct hostent *host_info;
	struct iovec iov[5];
	char buf[16];
	char str[16];
	char *tamanyo;

	//comprobamos longitud cola no sea mayor que el maximo
	if(strlen(cola)>=MAXQUEUENAME){
		perror("el nombre de la cola excede máximo permitido\n");
		return -1;
	}

	//comprobamos que el tamanyo del mensaje no sea mayor que el maximo
	if(tam>=MAXMSG){
		perror("el tamaño del mensaje excede máximo permitido\n");
		return 1;
	}

	//creamos socket
	if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		perror("error creando socket");
		return 1;
	}

	//nos conectamos con el broker sacando informacion de las variables de entorno
	host_info=gethostbyname(getenv("BROKER_HOST"));
	dir.sin_addr=*(struct in_addr *)host_info->h_addr;
	dir.sin_port=htons(atoi(getenv("BROKER_PORT")));	
	dir.sin_family=PF_INET;
	printf("puerto %d\n",dir.sin_port);
	if (connect(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
		perror("error en connect");
		close(s);
		return 1;
	}

	//creamos cabecera tipo : P&tamanyomensaje&tamanyonombrecola&nombrecola\n
	char cabecera[20]="P&";
	sprintf(str, "%d", tam);	//P&tamanyio
	strcat(cabecera,str);
	printf("cabecera : %s\n",cabecera);
	tamanyo=malloc(16);
	sprintf(tamanyo, "%ld%s", strlen(cola),"&");
	printf("mensaje : %p\n",mensaje);
	//introducimos la informacion en un iov
	iov[0].iov_base = strcat(cabecera,"&"); iov[0].iov_len = strlen(cabecera);
	fprintf(stdout,"iov[0]: %p\n",iov[0].iov_base);
	iov[1].iov_base = tamanyo; iov[1].iov_len = strlen(tamanyo);
	fprintf(stdout,"iov[1]: %p\n",iov[0].iov_base);
	iov[2].iov_base = (char *)cola; iov[2].iov_len = strlen(cola);
	fprintf(stdout,"iov[2]: %p\n",iov[2].iov_base);
	iov[3].iov_base = "\n"; iov[3].iov_len = 1;
	fprintf(stdout,"iov[3]: %p\n",iov[3].iov_base);

	//el mensaje va después de la cabecera ya que este puede ser en binario
	iov[4].iov_base = (char *)mensaje; iov[4].iov_len = tam; //introducimos
	fprintf(stdout,"iov[4]: %p\n",iov[4].iov_base);

	//enviamos la informacion
	writev(s, iov, 5);

	//esperamos la respuesta del broker, la recibimos y actuamos según sea esta
	while((leido=recv(s, buf, TAM,0))>0){
		printf("contenido buf: %s\n",buf);
		if(strcmp(buf,"OK")==0) {
			close(s);
			return 0;
		}
		else if(strcmp(buf,"FAIL")==0){
			printf("error metiendo mensaje en cola\n");
			close(s);
			return -1;
		}
		if (leido<0) {
			printf("error en read\n");
			close(s);
			return -1;
		}
	}
	free(tamanyo);
	close(s);
	return 0;
}
int get(const char *cola, void **mensaje, uint32_t *tam, bool blocking) {
	int s, leido, tamanyo_msg;
	struct sockaddr_in dir;
	struct hostent *host_info;
	struct iovec iov[3];
	char linea[12];
	char * ptr;
	FILE *desc_soc;

	//comprobamos longitud cola
	if(strlen(cola)>=MAXQUEUENAME){
		perror("el nombre de la cola excede máximo permitido\n");
		return -1;
	}

	//creamos socket
	if ((s=socket(PF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		perror("error creando socket");
		return -1;
	}

	host_info=gethostbyname(getenv("BROKER_HOST"));
	// 2 alternativas
	//memcpy(&dir.sin_addr.s_addr, host_info->h_addr, host_info->h_length);
	dir.sin_addr=*(struct in_addr *)host_info->h_addr;
	dir.sin_port=htons(atoi(getenv("BROKER_PORT")));	
	dir.sin_family=PF_INET;
	printf("puerto %d\n",dir.sin_port);
	if (connect(s, (struct sockaddr *)&dir, sizeof(dir)) < 0) {
		perror("error en connect");
		close(s);
		return 1;
	}

	//creamos cabecera tipo : G&nombrecola\n	
	char cabecera[3]="G";
	iov[0].iov_base = strcat(cabecera,"&"); iov[0].iov_len = strlen(cabecera);
	fprintf(stdout,"iov[0]: %p\n",iov[0].iov_base);
	iov[1].iov_base = (char *)cola; iov[1].iov_len = strlen(cola);
	fprintf(stdout,"iov[1]: %p\n",iov[1].iov_base);
	iov[2].iov_base = "\n"; iov[2].iov_len = 1;

	//enviamos la informacion
	writev(s, iov, 3);

	//leemos la cabecera del mensaje de vuelta (que recibimos del broker)
	desc_soc=fdopen(s, "r");
	setbuf(desc_soc, NULL);
	fgets(linea, 12, desc_soc);
	//sustituimos \n por \0 para tener un string como tal
	if( (ptr = strchr(linea, '\n')) != NULL)*ptr = '\0';
	//extraemos el tamanyo del mensaje
	tamanyo_msg=atoi(linea);
	// caso de cola no existente
	if(tamanyo_msg==-1) {
		printf("la cola no existe\n");
		return -1;
		}
	//caso cola vacia
	if(tamanyo_msg==0){
		printf("cola vacia\n");
		*tam=tamanyo_msg;
		return 0;
	}
	printf("tamanyo_msg: %d\n",tamanyo_msg);
	*mensaje=malloc(tamanyo_msg);
	*tam=tamanyo_msg;
	//leemos el mensaje que le hemos pedido al broker
	leido=recv(s, *mensaje, tamanyo_msg,MSG_WAITALL);
	if (leido<0) {
			perror("error en read");
			close(s);
			return 1;
	}

	printf("tam :%d",*tam);

	close(s);
	return 0;
}
