#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>

/***********************
 * @author Shaun Howard
 * EECS 338 as2
 */

//define numeric constants
//alpha offset is where the lowercase letters start in ascii
#define A_OFFSET 97
#define MAPPER_COUNT 4
#define REDUCER_COUNT 26
#define BUFF_SIZE 1024
#define READ 0
#define WRITE 1

// create mapper pipes array
int mapper_pipes[MAPPER_COUNT][2];

// create reducer pipes array
int reducer_pipes[REDUCER_COUNT][2];

/**
 * Wrapper function for file descriptor close with error-checking.
 * Given are the pipe and the ends the user wishes to close.
 * Enter 1 for read or write if you want to use either one.
 */
void closer(int pipe[2], int read, int write){
	//check to close read side
	if (read == 1)
		//error-check pipe
		if (close(pipe[READ]) != 0){
			perror("error closing read end of pipe...");
			fflush(stderr);
		}

	//check to close write side
	if (write == 1)
		//error-check pipe
		if (close(pipe[WRITE]) != 0){
			perror("error closing write end of pipe...");
			fflush(stderr);
		}
}

/**
 * Runs the mapper which reads input data from the
 * current pipe and checks if it's a lowercase letter.
 * If the letter is lowercase, it will write it to the
 * correct reducer pipe for counting.
 */
void Mapper(int curr_pipe){
	//close mapper pipe read ends if they come after this mapper
	int i =0;
	for (i=0; i<MAPPER_COUNT; i++){
		//close
		if (i>curr_pipe){
			//close read end of mapper, not this mapper
			closer(mapper_pipes[i], 1, 0);
		}
	}
	
	//close reducer pipe read ends
	for (i=0; i<REDUCER_COUNT;i++){
		closer(reducer_pipes[i], 1, 0);
	}

	//create buffer for read input character
	char buffer[BUFF_SIZE] = {0};

	read(mapper_pipes[curr_pipe][READ], buffer, BUFF_SIZE);

	//read input data from pipe and inspect the characters
	for (i=0; i<strlen(buffer); i++){
		//check if character is lower-case, if so, send to correct reducer
		if (97 <= buffer[i] && buffer[i] <= 122){
			//find reducer for given character
			int reducer = buffer[i] - A_OFFSET;

			//pass character to the correct reducer for that character
			write(reducer_pipes[reducer][WRITE], &buffer[i], 1);
		}
	}

	//close reducer pipes write ends to signal end of input
	for (i = 0; i < REDUCER_COUNT; i++)
		closer(reducer_pipes[i], 0, 1);

	fflush(stdout);
	_exit(EXIT_SUCCESS);
}

/**
 * Runs the reducer which counts the occurrences of the character it
 * is designed to check for given char_to_count. The number of the
 * current reducer represents its place in the reducer pipe array.
 * It reads from its pipe and counts the number of characters
 * it has been fed via the pipe. It will close its pipe and
 * exit with success if counting worked or exit with an error.
 */
void Reducer(int curr_reducer, char char_to_count){
	fflush(stdout);
	int i=0;
	//close mapper pipe write ends (read ends already closed)
	for (i=0; i<MAPPER_COUNT; i++){
		closer(mapper_pipes[i], 0, 1);
	}

	for (i=0; i<REDUCER_COUNT; i++){
		if (i!=curr_reducer){
			//close read end of reducer pipe
			closer(reducer_pipes[i], 1, 0);
		}
	}

	//use an unsigned char so no overflow occurs (only dealing with lower-case letters)
	unsigned char buffer;
	int char_count = 0;
	//read input data from pipe and inspect one character at a time
	while (read(reducer_pipes[curr_reducer][READ], &buffer, 1) > 0) {
		//check if character is intended for this reducer
		if (buffer == char_to_count){
			char_count++;
		} else {
			//character does not match this reducer
			perror("error in reducer, wrong character encountered in pipe");
			fflush(stderr);
			closer(reducer_pipes[curr_reducer], 1, 0);
			_exit(EXIT_FAILURE);
		}
	}

	//close read end of reducer pipe
	closer(reducer_pipes[curr_reducer], 1, 0);

	//print character count
	printf("count %c: %d\n", char_to_count, char_count);
	fflush(stdout);
	_exit(EXIT_SUCCESS);
}

/**
 * Opens the input file, reads and distributes
 * each line to a mapper which uses reducers
 * to count the number of occurrences of each
 * possible lower case letter in the line.
 * Returns 0 on success, 1 on error.
 */
int read_and_distribute_lines(){
	int i;
	//create mapper pipes
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
	for (i=0; i<REDUCER_COUNT; i++){
		int status = pipe(reducer_pipes[i]);
		//error check pipe
		if (status < 0){
			perror("Failure in creating reducer pipe...");
			fflush(stderr);
			return 1;
		}
	}

	//store mapper pids
	pid_t mappers[MAPPER_COUNT];

	int curr_pipe;
	//fork 4 mappers for reading each line of input
	for (curr_pipe = 0; curr_pipe < MAPPER_COUNT; curr_pipe++){
		//fork mapper
		mappers[curr_pipe] = fork();

		//determine if the child has control, error forking, or the parent has control
		if (mappers[curr_pipe] == 0){
			//close mapper pipe write end
			closer(mapper_pipes[curr_pipe], 0, 1);
			//run mapper process if child
			Mapper(curr_pipe);
		} else if (mappers[curr_pipe] == -1){
			//error check fork
			perror("fork");
			return 1;
		} else {
			//parent closes read end of mapper pipe
			closer(mapper_pipes[curr_pipe], 1, 0);
		}
	}

	//store reducer pids
	pid_t reducers[REDUCER_COUNT];

	//fork 26 reducers for counting occurrences of each letter in input
	for (curr_pipe = 0; curr_pipe < REDUCER_COUNT; curr_pipe++){
		//find the character the reducer will count
		char char_to_count = A_OFFSET + curr_pipe;

		//fork reducer
		reducers[curr_pipe] = fork();

		//close write end of reducer pipe (parent and child)
		closer(reducer_pipes[curr_pipe], 0, 1);

		//run reducer process if child
		if (reducers[curr_pipe] == 0){
			//create a reducer to count occurrences of a specific character in input
			Reducer(curr_pipe, char_to_count);
		} else if (reducers[curr_pipe] == -1){
			//error check fork
			perror("fork");
			return 1;
		} else {

		}
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
	while(fgets(line, BUFF_SIZE, input) > 0 && curr_pipe < MAPPER_COUNT){
		//write line to mapper pipe, read end already closed
		write(mapper_pipes[curr_pipe][WRITE], line, BUFF_SIZE);

		//close write side of pipe
		closer(mapper_pipes[curr_pipe], 0, 1);
		curr_pipe++;
	}

	//close file
	fclose(input);

	//wait for each child to exit
	for (i = 0; i < MAPPER_COUNT; i++){
	    wait(&mappers[i]);
	}

	//wait for each child to exit
	for (i = 0; i < REDUCER_COUNT; i++)
		wait(&reducers[i]);

	return 0;
}

int main(void){
	//open file, read lines and distribute them across mappers and count
	//letter occurrences with reducers, which then print to console the count
	//of each letter in the input file
	if(read_and_distribute_lines() == 1)
		exit(EXIT_FAILURE);
	exit(EXIT_SUCCESS);
}
