#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#define IS_ODD(x) ((x % 2))

void message(int *outgoing, int *incoming, int myid, int buff_count, int num_procs, int rows, int dir) {
    int next;
    int prev;
    MPI_Status stat;
    /* printf("num_procs: %d\n", num_procs); */
    next = (myid + 1) % num_procs;
    prev = ((myid == 0) ? (num_procs - 1) : (myid - 1));
    if (dir != 1) {
        next = next + prev;
        prev = next - prev;
        next = next - prev;
    }

    if (IS_ODD(myid)) {

        /* printf("1 ID: %d message sending to %d...\n", myid, next); */
        MPI_Send(outgoing, buff_count, MPI_INT, next, 3, MPI_COMM_WORLD);    
        /* printf("2 ID: %d message receiving from %d...\n", myid, prev); */
        MPI_Recv(incoming, buff_count, MPI_INT, prev, 3, MPI_COMM_WORLD, &stat);
    } else {
        /* printf("3 ID: %d message receiving from %d...\n", myid, prev); */
        /* printf("ID: %d NEXT: %d PREV: %d DIR:%d\n", myid, next, prev, dir); */
        MPI_Recv(incoming, buff_count, MPI_INT, prev, 3, MPI_COMM_WORLD, &stat);
        /* printf("4 ID: %d message sending to %d...\n", myid, next); */
        /* printarray(outgoing, 16); */
        MPI_Send(outgoing, buff_count, MPI_INT, next, 3, MPI_COMM_WORLD);
    }
}
void trim(int low, int high, int *mygrid, int dim) {
    int i;
    for (i = 0; i < dim * dim; i++)
        if (!(i >= low * dim && i < high * dim))
            mygrid[i] = 0;

} 
void update(int low, int high, int *mygrid, int *tempgrid, int dim) {
    int i = 0;
    int j = 0;
    int jleft, jright, iup ,idown;
    // XXX FIXME XXX FIXME
    int count = 0;
    int ds = dim * dim;
    for (i = 0; i < dim * dim; i++)
        tempgrid[i] = 0;
    for (i = low; i < high; i++) {
        for (j = 0; j < dim; j++) {
            int ij = i * dim + j;
            
            count = 0;
            if (j == 0)
                jleft = dim - 1;
            else 
                jleft = j - 1;
            if (j == dim - 1)
                jright = 0;
            else 
                jright = j + 1;
            if (i == 0)
                iup = dim - 1;
            else 
                iup = i - 1;
            if (i == dim - 1) 
                idown = 0;
            else 
                idown = i + 1;
            
            count = mygrid[iup * dim + jleft] + mygrid[iup * dim + j] + mygrid[iup * dim + jright] + mygrid[i * dim + jleft] + mygrid[i * dim + jright] + mygrid[idown * dim + jleft] + mygrid[idown * dim + j] + mygrid[idown * dim + jright];
            /* if (ij == 1) { */
                /* printf("IUP: %d, IDOWN: %d, JLEFT: %d, JRIGHT: %d\n", iup, idown, jleft, jright); */
                /* printf("COUNT: %d\n", count); */
            /* } */

            tempgrid[ij] = mygrid[ij];
            if (mygrid[ij] == 0 && count == 3) {
                tempgrid[ij] = 1;
            } else if (mygrid[ij] == 1 && (count == 2 || count == 3)) {
                tempgrid[ij] = 1;
            } else {
                tempgrid[ij] = 0;
            }
        } 
    }
    for (i = 0; i < dim * dim; i++)
        mygrid[i] = tempgrid[i];
}
void printgrid(int *grid, int dim) {
    int i, j;

    printf("=================================\n");
    for (i = 0; i < dim; i++) {
        for (j = 0; j < dim; j++) {
            printf("%d ", grid[i * dim + j]);
        }
        printf("\n");
    }
    printf("=================================\n");
    fflush(stdout);
}
void printarray(int *x, int dim) {
    int i;

    printf("=================================\n");
    for (i = 0; i < dim; i++) {
        printf("%d ", x[i]);
    }
    printf("\n=================================\n");
}
void copy(int *src, int *dest, int srcfrom, int srcto, int destfrom, int destto) {
    int i;
    /* printf("srcfrom: %d srcto: %d destfrom: %d destto: %d\n", srcfrom, srcto, destfrom, destto); */
    if ((srcto - srcfrom) != (destto - destfrom)) {
        printf("srcto: %d srcfrom: %d, destto: %d, destfrom: %d\n", srcto, srcfrom, destto, destfrom);
    }
    assert((srcto - srcfrom) == (destto - destfrom));
    for (i = 0; i < srcto - srcfrom; i++) {
        dest[i + destfrom] = src[i + srcfrom];
    }
    fflush(stdout);
}

int main(int argc, char **argv) {
    const int dim = 16;
    int mygrid[ 256 ] = { 
                            0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 
                        };
    int *tempgrid;
    int id;
    int numprocs = 0;
    int low = 0, high = 0;
    int *outgoing, *incoming;
    int i;
    double t0;
    int itr = 0;
    MPI_Status stat;



    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &id);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    if (numprocs != 1 && numprocs != 2 && numprocs != 4 && numprocs != 8 && numprocs != 16) {
        printf("Only support 1/2/4/8/16 processors\n");
        fflush(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    /* printf("NUMPROC: %d\n", numprocs); */
    int myrows = dim / numprocs;
    low = id * myrows;
    high = low + myrows;
    

    trim(low, high, mygrid, dim);

    outgoing = (int *) malloc(dim * sizeof(int));
    incoming = (int *) malloc(dim * sizeof(int));
    tempgrid = (int *) malloc(dim * dim * sizeof(int));
    /* printgrid(mygrid, dim); */
    for (itr = 0; itr < 64; itr++) {

        /* printf("ID: %d COPY TO OUTGOING GOING DOWN\n", id); */
        if (numprocs != 1) {
            printf("1");
            copy(mygrid, outgoing, (high - 1) * dim == 0 ? 256 : (high - 1) * dim, high * dim, 0, dim);

            message(outgoing, incoming, id, dim, numprocs, myrows, 1);
            /* printf("ID: %d COPYBACK TO MYGRID UP\n", id); */
            printf("2");
            copy(incoming, mygrid, 0, dim, ((low - 1 + dim) % dim) * dim, low * dim == 0 ? 256 : low * dim);
            
            /* printarray(incoming, dim); */
            MPI_Barrier(MPI_COMM_WORLD);


            /* printf("ID: %d COPY TO OUTGOING GOING UP\n", id); */
            printf("3");
            copy(mygrid, outgoing, low * dim, ((low + 1) % dim) * dim, 0, dim);
            /* printgrid(mygrid, dim); */
            message(outgoing, incoming, id, dim, numprocs, myrows, -1);
            printf("4");
            /* printf("ID: %d COPYBACK TO MYGRID DOWN\n", id); */
            copy(incoming, mygrid, 0, dim, (high * dim == 256) ? 0 : high * dim, ((high + 1) % dim) * dim);
            /* if (id == 1) { */
                /* printf("FROM: %d TO: %d\n", (high * dim == 256) ? 0 : high * dim, ((high + 1) % dim) * dim); */
                /* printarray(incoming, dim); */
                /* fflush(stdout); */
            /* } */
            /* #<{(| printgrid(mygrid, dim); |)}># */
            /* printarray(incoming, dim); */
            MPI_Barrier(MPI_COMM_WORLD);
            /* if (id == 1) { */
                /* printgrid(mygrid, dim); */
            /* } */
        }
        /* printf("ID: %d UPDATING FROM: %d TO: %d\n", id, low, high); */
        /* if (id == 0) { */
        /*     printgrid(mygrid, dim); */
        /*     printf("LOW: %d HIGH: %d\n", low, high); */
        /*     fflush(stdout); */
        /* } */
        update(low, high, mygrid, tempgrid, dim);
        trim(low, high, mygrid, dim);
        if (numprocs != 1) {
            if (id == 0) {
                int proc;
                int i;
                /* printf("ID0, receiving result...\n"); */
                for (proc = 1; proc < numprocs; proc++) {
                    MPI_Recv(tempgrid, dim * dim, MPI_INT, proc, 3, MPI_COMM_WORLD, &stat);
                    /* printgrid(tempgrid, dim); */
                    for (i = 0; i < dim * dim; i++)
                        mygrid[i] += tempgrid[i];
                    /* printgrid(tempgrid, dim); */
                }
            } else {
                /* printf("ID%d, sending result...\n", id); */
                /* printgrid(tempgrid, dim); */
                /* printgrid(mygrid, dim); */
                MPI_Send(mygrid, dim * dim, MPI_INT, 0, 3, MPI_COMM_WORLD);
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
        if (id == 0) {
            printf("iteration: %d\n", itr);
            printgrid(mygrid, dim);
        }
    }
    MPI_Finalize();
}
