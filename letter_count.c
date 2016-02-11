#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

#define A_OFFSET 97
#define MAPPER_COUNT 4
#define REDUCER_COUNT 26
#define BUFF_SIZE 1024
#define READ 0
#define WRITE 1

/**
 * Runs the mapper process, which creates 26 reducers to count all lowercase letters.
 */
void Mapper(int mapper_pipes[2], int reducer_pipes[REDUCER_COUNT][2]){

	char buffer;
	//read input data from pipe and inspect the characters
	while (read(mapper_pipes[0], &buffer, 1) > 0) {
		//convert character to number
		int character = atoi(&buffer);

		//find reducer for given character
		int reducer = character - A_OFFSET;

		//pass character to the correct reducer for that character
		write(reducer_pipes[reducer][WRITE], &buffer, 1);
	    sleep(1);
	}

	//close reducer pipes write ends to signal end of input
	int i;
	for (i = 0; i < REDUCER_COUNT; i++)
		close(reducer_pipes[i][WRITE]);

	exit(EXIT_SUCCESS);
}

void Reducer(int reducer_pipe[2], char char_to_count){
	char buffer;
	int char_count = 0;
	//read input data from pipe and inspect one character at a time
	while (read(reducer_pipe[0], &buffer, 1) > 0) {
		//check if character is intended for this reducer
		if (buffer == char_to_count)
			char_count++;
		else {
			//character does not match this reducer
			perror("error in reducer, wrong character encountered in pipe");
			fflush(stderr);
			close(reducer_pipe[READ]);
			exit(EXIT_FAILURE);
		}
	    sleep(1);
	}

	//close read end of pipe
	close(reducer_pipe[READ]);

	//print character count
	printf("count %c: %d", char_to_count, char_count);
	fflush(stdout);
	exit(EXIT_SUCCESS);
}

/**
 * Opens the input file, reads and distributes
 * each line to a mapper which uses reducers
 * to count the number of occurrences of each
 * possible lower case letter in the line.
 * Returns 0 on success, 1 on error.
 */
int open_and_distribute_lines(){
	//create mapper pipes
	int mapper_pipes[MAPPER_COUNT][2];
	int i;
	for (i=0; i<MAPPER_COUNT; i++){
		int status = pipe(mapper_pipes[i]);
		//error check pipe
		if (status < 0){
			perror("Failure in creating mapper pipe...");
			fflush(stderr);
			return 1;
		}
	}

	//create reducer pipes
	int reducer_pipes[REDUCER_COUNT][2];
	for (i=0; i<REDUCER_COUNT; i++){
		int status = pipe(reducer_pipes[i]);
		//error check pipe
		if (status < 0){
			perror("Failure in creating reducer pipe...");
			fflush(stderr);
			return 1;
		}
	}

	printf("created mapper and reducer pipes");

	//store mapper pids
	pid_t mappers[MAPPER_COUNT];

	int curr_pipe;
	//fork 4 mappers for reading each line of input
	for (curr_pipe = 0; curr_pipe < MAPPER_COUNT; curr_pipe++){
		//fork mapper
		pid_t mapper = fork();

		//determine if the child has control, error forking, or the parent has control
		if (mapper == 0){
			//close pipe write end
			close(mapper_pipes[curr_pipe][WRITE]);
			//run mapper process if child
			Mapper(mapper_pipes[curr_pipe], reducer_pipes);
		} else if (mapper == -1){
			//error check fork
			perror("fork");
			return 1;
		} else {
			//parent closes read end of pipe
			close(mapper_pipes[curr_pipe][READ]);
		}
		//store pid of mapper
		mappers[curr_pipe] = mapper;
	}

	//store reducer pids
	pid_t reducers[REDUCER_COUNT];

	//fork 26 reducers for counting occurrences of each letter in input
	for (curr_pipe = 0; curr_pipe < REDUCER_COUNT; curr_pipe++){
		//fork reducer
		pid_t reducer = fork();

		//find the character the reducer will count
		char char_to_count = A_OFFSET + curr_pipe;

		//run reducer process if child
		if (reducer == 0){
			//create a reducer to count occurrences of a specific character in input
			Reducer(reducer_pipes[curr_pipe], char_to_count);
		} else if (reducer == -1){
			//error check fork
			perror("fork");
			return 1;
		}
		//store pid of reducer
		reducers[curr_pipe] = reducer;
	}

	//read file and error check result
	FILE *input = fopen("input.txt", "r");
	if (input == NULL){
		perror("fopen");
		fflush(stderr);
		return 1;
	}

	// create space for line
	char line[BUFF_SIZE];
	curr_pipe = 0;

	//divide input file among mappers, sending each mapper one line via a pipe
	while(fgets(line, BUFF_SIZE, input) > 0 && curr_pipe < 4){
		//write line to mapper pipe, read end already closed
		write(mapper_pipes[curr_pipe][WRITE], line, BUFF_SIZE);

		//close write side of pipe
		close(mapper_pipes[curr_pipe][WRITE]);
		curr_pipe++;
	}

	//close file
	fclose(input);

	//wait for each child to exit
	for (i = 0; i < MAPPER_COUNT; i++)
		wait(&mappers[i]);

	//close pipes to reducers
	for (curr_pipe = 0; curr_pipe < REDUCER_COUNT; curr_pipe++){
		//parent process, close both ends of reducer pipes
		close(reducer_pipes[curr_pipe][READ]);
		close(reducer_pipes[curr_pipe][WRITE]);
	}

	//wait for each child to exit
	for (i = 0; i < REDUCER_COUNT; i++)
		wait(&reducers[i]);

	return 0;
}

int main(void){
	//open file, read lines and distribute them across mappers and reducers
	if(open_and_distribute_lines() == 1)
		exit(EXIT_FAILURE);
	exit(EXIT_SUCCESS);
}
