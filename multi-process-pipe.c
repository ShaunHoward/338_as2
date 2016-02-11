#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

//int main(int argc, char **argv) {
//    int fd[2];
//    pid_t child_pid;
//    char buf;
//
//    pipe(fd);
//
//    child_pid = fork();
//
//    if (child_pid == 0) {
//
//        sleep(1);
//        close(fd[1]);
//
//        while (read(fd[0], &buf, 1) > 0) {
//            printf("next char: %c\n", buf);
//            sleep(1);
//        }
//
//        close(fd[0]);
//
//        printf("child terminating\n");
//        exit(EXIT_SUCCESS);
//    } else {
//        close(fd[0]);
//
//        //write(fd[1], argv[1], strlen(argv[1]));
//        char buffer[1024];
//        while (strcmp(gets(buffer), "exit") != 0) {
//            write(fd[1], buffer, strlen(buffer));
//        }
//        close(fd[1]);
//        wait(NULL);
//        printf("parent terminating\n");
//        exit(EXIT_SUCCESS);
//    }
//}






        

