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
#define MAPPER_COUNT 1
#define REDUCER_COUNT 26
#define BUFF_SIZE 1024
#define READ 0
#define WRITE 1

// create mapper pipes array
int mapper_pipes[MAPPER_COUNT][2];

// create reducer pipes array
int reducer_pipes[REDUCER_COUNT][2];

/**
 * Runs the mapper which reads input data from the
 * current pipe and checks if it's a lowercase letter.
 * If the letter is lowercase, it will write it to the
 * correct reducer pipe for counting.
 */
void Mapper(int curr_pipe){
	//create buffer for read input character
	char buffer;

	//read input data from pipe and inspect the characters
	while (read(mapper_pipes[curr_pipe][READ], &buffer, 1) > 0) {
		//convert character to number
		int character = buffer;

		//check if character is lower-case, if so, send to correct reducer
		if (97 <= character && character <= 122){
			//find reducer for given character
			int reducer = character - A_OFFSET;

			printf("curr buff: %s\n", &buffer);
			fflush(stdout);

			//close read end of pipe
			close(reducer_pipes[reducer][READ]);

			//pass character to the correct reducer for that character
			write(reducer_pipes[reducer][WRITE], &buffer, 1);
		}
		printf("stuck\n\n");
	}
	sleep(2);
	printf("I got here");

	//close reducer pipes write ends to signal end of input
	int i;
	for (i = 0; i < REDUCER_COUNT; i++)
		close(reducer_pipes[i][WRITE]);

	printf("at exit");
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

	//close write end of reducer pipe to assure no writes
	close(reducer_pipes[curr_reducer][WRITE]);

	//use an unsigned char so no overflow occurs (only dealing with lower-case letters)
	unsigned char buffer;
	int char_count = 0;
	//read input data from pipe and inspect one character at a time
	while (read(reducer_pipes[curr_reducer][READ], &buffer, 1) > 0) {
		printf("buffer: %c\n", buffer);
		fflush(stdout);
		//check if character is intended for this reducer
		if (buffer == char_to_count){
			char_count++;
		} else {
			//character does not match this reducer
			perror("error in reducer, wrong character encountered in pipe");
			fflush(stderr);
			close(reducer_pipes[curr_reducer][READ]);
			_exit(EXIT_FAILURE);
		}
	}
	printf("I hit this reducer point");
	fflush(stdout);
	//close read end of pipe
	close(reducer_pipes[curr_reducer][READ]);

	//print character count
	printf("count %c: %d", char_to_count, char_count);
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
			//close pipe write end
			close(mapper_pipes[curr_pipe][WRITE]);
			//run mapper process if child
			Mapper(curr_pipe);
		} else if (mappers[curr_pipe] == -1){
			//error check fork
			perror("fork");
			return 1;
		} else {
			//parent closes read end of pipe
			close(mapper_pipes[curr_pipe][READ]);
		}
	}

	//store reducer pids
	pid_t reducers[REDUCER_COUNT];

	//fork 26 reducers for counting occurrences of each letter in input
	for (curr_pipe = 0; curr_pipe < REDUCER_COUNT; curr_pipe++){
		//fork reducer
		reducers[curr_pipe] = fork();

		//find the character the reducer will count
		char char_to_count = A_OFFSET + curr_pipe;

		//run reducer process if child
		if (reducers[curr_pipe] == 0){
			close(reducer_pipes[curr_pipe][WRITE]);
			//create a reducer to count occurrences of a specific character in input
			Reducer(curr_pipe, char_to_count);
		} else if (reducers[curr_pipe] == -1){
			//error check fork
			perror("fork");
			return 1;
		} else {
			close(reducer_pipes[curr_pipe][READ]);
			close(reducer_pipes[curr_pipe][WRITE]);
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
	while(fgets(line, BUFF_SIZE, input) > 0 && curr_pipe < 4){
		//close write side of pipe
		close(mapper_pipes[curr_pipe][READ]);

		//write line to mapper pipe, read end already closed
		write(mapper_pipes[curr_pipe][WRITE], line, BUFF_SIZE);

		//close write side of pipe
		close(mapper_pipes[curr_pipe][WRITE]);
		curr_pipe++;
	}

	//close file
	fclose(input);

	//wait for each child to exit
	for (i = 0; i < MAPPER_COUNT; i++){
		printf("I am a waiting parent");
	    fflush(stdout);
		wait(&mappers[i]);
	}

//	//close pipes to reducers
//	for (curr_pipe = 0; curr_pipe < REDUCER_COUNT; curr_pipe++){
//		//parent process, close both ends of reducer pipes
//		close(reducer_pipes[curr_pipe][READ]);
//		close(reducer_pipes[curr_pipe][WRITE]);
//	}

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
