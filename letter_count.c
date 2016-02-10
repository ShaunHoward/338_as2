#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define A 'a'
#define MAPPER_COUNT 4
#define REDUCER_COUNT 26
#define BUFF_SIZE 32
#define READ 0
#define WRITE 1

/**
 * Runs the mapper process, which creates 26 reducers to count all lowercase letters.
 */
void Mapper(int mapper_pipes[MAPPER_COUNT][2]){
	//create and run a reducer process for each lower case character in the alphabet
	for (int i = 0; i < REDUCER_COUNT; i++){
		//fork a new child reducer process
		pid_t reducer = fork();
		if (reducer == 0){
			int count = 0;
			//run a mapper process for each line of the input file, i.e. MAPPER_COUNT
			for (int j = 0; j < MAPPER_COUNT; j++){
				char* line = malloc(BUFF_SIZE);
				//read a line from the current mapper pipe
				int num_bytes = read(mapper_pipes[j][READ], line, BUFF_SIZE);
				if (num_bytes == BUFF_SIZE){
					for (int k = 0; k < BUFF_SIZE; k++){
						//check if the line character at k is the current reducer letter
						if (line[k] == A + i){
							//increment count if we have found a matching letter
							count++;
						}
					}
				}
				//free line
				if (line != NULL)
					free(line);
			}
			char reducer_char = A + i;
			//quick way to print the character count
			printf("Number of %c: %d", reducer_char, count);
			fflush(stdout);
			exit(EXIT_SUCCESS);
		}
	}
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
	//read file and error check result
	FILE *input = fopen("input.txt", "r");
	if (input == NULL){
		perror("fopen");
		fflush(stderr);
		return 1;
	}

	//create pipes
	int mapper_pipes[MAPPER_COUNT][2];
	for (int i=0; i<MAPPER_COUNT; i++){
		int status = pipe(mapper_pipes[i]);
		if (status < 0){
			perror("Failure in creating pipe...");
			fflush(stderr);
			exit(EXIT_FAILURE);
		}
	}

	//allocate variables for reading file lines
	pid_t mappers[MAPPER_COUNT];
	size_t len = 0;
	ssize_t chars_read;
	char *line;
	int curr_pipe = 0;

	//read lines and distribute via pipes to mappers
	while((chars_read=getline(&line, &len, input))!=-1 && curr_pipe < 4){
		//close read side of pipe
		close(mapper_pipes[curr_pipe][READ]);

		//write line to mapper pipe
		write(mapper_pipes[curr_pipe][WRITE], line, BUFF_SIZE);

		//close pipe write end
		close(mapper_pipes[curr_pipe][WRITE]);
		pid_t mapper = fork();

		//run mapper process if child
		if (mapper == 0){
			Mapper(mapper_pipes);
		} else {
			//close both ends of the pipe
			close(mapper_pipes[curr_pipe][WRITE]);
			close(mapper_pipes[curr_pipe][READ]);
			//store pid of mapper
			mappers[curr_pipe] = mapper;
		}
	}

	//close file and free dynamic memory
	fclose(input);
	if (line){
		free(line);
	}

	//wait for each child to exit
	for (int i = 0; i < MAPPER_COUNT; i++)
		wait(&mappers[i]);

	return 0;
}

int main(void){
	//open file, read lines and distribute them across mappers and reducers
	if(open_and_distribute_lines() == 1)
		exit(EXIT_FAILURE);
	exit(EXIT_SUCCESS);
}
