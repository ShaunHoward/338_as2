#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#define BUFFER_SIZE 1024
#define ALPHA_OFFSET 97 // ascii code for "a"
#define LETTERS 26 // letters in the alphabet

/**
 *
 * Print counts for lowercase letters in the input file.
 *
 */
int main(void){
  int i; // iterator
  int counts[LETTERS] = {0}; // storage for letter counts, initialized to 0
  char buffer[BUFFER_SIZE]; // a buffer for each line from the file
  FILE *input_file = fopen("input.txt", "r");
  // step: read the input file and count lowercase letters
  while(fgets(buffer, BUFFER_SIZE, input_file) > 0){
    printf("line: %s\n", buffer);
    for(i = 0; i < strlen(buffer); i++){
      if(buffer[i] >= ALPHA_OFFSET && buffer[i] < ALPHA_OFFSET + LETTERS){
        counts[buffer[i] - ALPHA_OFFSET]++;
      }
    }
  }
  fclose(input_file);
  // step: write the counts to stdout
  for(i = 0; i < LETTERS; i++){
    printf("count %c: %d\n", i + ALPHA_OFFSET, counts[i]);
  }
  exit(EXIT_SUCCESS);
}
