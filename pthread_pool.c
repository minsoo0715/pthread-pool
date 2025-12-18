/*
 * Copyright(c) 2021-2024 All rights reserved by Heekuck Oh.
 * 이 프로그램은 한양대학교 ERICA 컴퓨터학부 학생을 위한 교육용으로 제작되었다.
 * 한양대학교 ERICA 학생이 아닌 이는 프로그램을 수정하거나 배포할 수 없다.
 * 프로그램을 수정할 경우 날짜, 학과, 학번, 이름, 수정 내용을 기록한다.
 *
 * 06.05. 전자공학부 2022000500 조민수
 * pthread를 통한 thread_pool 구현
 * static void pthread_pool_update_state() 함수 추가
 */
#include <stdlib.h>
#include "pthread_pool.h"

/* pool의 state 변수를 안전하게 변경하기 위한 함수
 * lock을 걸고 state를 업데이트함
 */
static void pthread_pool_update_state(pthread_pool_t *pool, int state) {
    pthread_mutex_lock(&pool->mutex);
    pool->state = state;
    pthread_mutex_unlock(&pool->mutex);
}

/*
 * 풀에 있는 일꾼(일벌) 스레드가 수행할 함수이다.
 * FIFO 대기열에서 기다리고 있는 작업을 하나씩 꺼내서 실행한다.
 * 대기열에 작업이 없으면 새 작업이 들어올 때까지 기다린다.
 * 이 과정을 스레드풀이 종료될 때까지 반복한다.
 */
static void *worker(void *param)
{
    pthread_pool_t *pool = (pthread_pool_t*) param;

    // 실행할 task를 임시 저장할 변수
    task_t current_task;

    // 스레드풀이 종료될 때까지 실행
    while(pool->state != OFF) {
        pthread_mutex_lock(&pool->mutex);

        while (pool->q_len == 0) {
            // OFF인 경우 바로 종료, EXIT인 경우도 바로 종료 (이미 q_len은 0임)
            if(pool->state == OFF || pool->state == EXIT) {
                pthread_mutex_unlock(&pool->mutex);
                pthread_exit(NULL);
            }
            // pool의 task가 들어 올때까지 대기
            pthread_cond_wait(&pool->full, &pool->mutex);
        }
        // CS 시작

        // task 큐에서 소비
        current_task = pool->q[pool->q_front];
        pool->q_front = (pool->q_front + 1) % pool->q_size;
        pool->q_len--;



        // 하나를 소비했음을 알림
        pthread_cond_signal(&pool->empty);

        // CS 종료
        pthread_mutex_unlock(&pool->mutex);

        // 현재 태스크 실행
        current_task.function(current_task.param);


    }
    pthread_exit(NULL);
}



/*
 * 스레드풀을 생성한다. bee_size는 일꾼(일벌) 스레드의 개수이고, queue_size는 대기열의 용량이다.
 * bee_size는 POOL_MAXBSIZE를, queue_size는 POOL_MAXQSIZE를 넘을 수 없다.
 * 일꾼 스레드와 대기열에 필요한 공간을 할당하고 변수를 초기화한다.
 * 일꾼 스레드의 동기화를 위해 사용할 상호배타 락과 조건변수도 초기화한다.
 * 마지막 단계에서는 일꾼 스레드를 생성하여 각 스레드가 worker() 함수를 실행하게 한다.
 * 대기열로 사용할 원형 버퍼의 용량이 일꾼 스레드의 수보다 작으면 효율을 극대화할 수 없다.
 * 이런 경우 사용자가 요청한 queue_size를 bee_size로 상향 조정한다.
 * 성공하면 POOL_SUCCESS를, 실패하면 POOL_FAIL을 리턴한다.
 */
int pthread_pool_init(pthread_pool_t *pool, size_t bee_size, size_t queue_size)
{
    // 일꾼 스레드 수 또는 대기열의 크기가 최대 크기보다 크다면 FAIL
    if(bee_size > POOL_MAXBSIZE || queue_size > POOL_MAXQSIZE)
        return POOL_FAIL;


    /* mutex 및 공유 변수 초기화 */
    pthread_mutex_init(&pool->mutex, NULL);
    pthread_cond_init(&pool->full, NULL);
    pthread_cond_init(&pool->empty, NULL);

    pool->bee_size = bee_size;
    // 대기열 큐의 사이즈는 일꾼 스레드 수보다 같거나 커야함
    pool->q_size = queue_size < bee_size ? bee_size : queue_size;

    // 대기열 큐 관련 변수 초기화
    pool->q_front = 0;
    pool->q_len = 0;

    // 크기에 맞게 동적 배열 할당
    pool->bee = (pthread_t*) malloc(sizeof(pthread_t) * pool->bee_size);
    pool->q = (task_t*) malloc(sizeof(task_t) * pool->q_size);

    // 실행할 준비가 완료됨
    pool->state = STANDBY;

    // 스레드 생성
    for(int i = 0; i < pool->bee_size; ++i)
        if (pthread_create(pool->bee + i, NULL, worker, pool))
            return POOL_FAIL;

    return POOL_SUCCESS;
}

/*
 * 스레드풀에서 실행시킬 함수와 인자의 주소를 넘겨주며 작업을 요청한다.
 * 스레드풀의 대기열이 꽉 찬 상황에서 flag이 POOL_NOWAIT이면 즉시 POOL_FULL을 리턴한다.
 * POOL_WAIT이면 대기열에 빈 자리가 나올 때까지 기다렸다가 넣고 나온다.
 * 작업 요청이 성공하면 POOL_SUCCESS를 그렇지 않으면 POOL_FAIL을 리턴한다.
 */
int pthread_pool_submit(pthread_pool_t *pool, void (*f)(void *p), void *p, int flag)
{
    // 이미 종료중인 경우 더 이상 submit을 받지 않는다.
    if(pool->state == EXIT || pool->state == OFF)
        return POOL_FAIL;

    // 대기중인 thread pool이였다면 ON 상태로 변경
    if(pool->state == STANDBY)
        pool->state = ON;

    pthread_mutex_lock(&pool->mutex);
    // CS 시작 (pool->q_size 및 pool->q_len은 공유되고 있으므로)
    /*
     * POOL_NOWAIT 일 때는 대기열이 꽉차면 POOL_FULL을 반환
     *
     * flag에 대한 비교를 먼저하는 방식을 적용해
     * short-circuit evaluation에 의한 최적화를 기대할 수 있음
     */
    if(flag == POOL_NOWAIT && pool->q_size == pool->q_len) {
        pthread_mutex_unlock(&pool->mutex);
        return POOL_FULL;
    }

    /*
     * flag가 POOL_NOWAIT인 경우 위에서 이미 대기열에 비어 있음을 체크했으므로
     * 해당 while문에 들어가서 대기하는 경우가 존재하지 않음
     *
     * flag == POOL_WAIT인 경우 대기열이 비는 것을 대기하거나, 이미 비어있는 경우 바로 submit됨
     */
    while(pool->q_size == pool->q_len) {
        // 대기열이 비어지기를 기다리는 중에 state가 OFF 또는 EXIT이라면 lock을 풀고 나감
        if(pool->state == OFF || pool->state == EXIT) {
            pthread_mutex_unlock(&pool->mutex);
            return POOL_FAIL;
        }
        pthread_cond_wait(&pool->empty, &pool->mutex); // 대기열에 task가 가득 차 있으면 대기
    }
    /*
     * 큐의 가장 뒷 부분에 task를 넣음
     * pool->q_front + pool->q_len은 큐의 가장 뒷부분을 가리킴 (원형 큐 이므로 MOD 연산)
     */
    int pos = (pool->q_front + pool->q_len) % pool->q_size;
    pool->q[pos].function = f;
    pool->q[pos].param = p;
    pool->q_len++;

    // task가 대기열에 들어왔음을 알림
    pthread_cond_signal(&pool->full);

    // CS 끝
    pthread_mutex_unlock(&pool->mutex);


    return POOL_SUCCESS;
}

/*
 * 스레드풀을 종료한다. 일꾼 스레드가 현재 작업 중이면 그 작업을 마치게 한다.
 * how의 값이 POOL_COMPLETE이면 대기열에 남아 있는 모든 작업을 마치고 종료한다.
 * POOL_DISCARD이면 대기열에 새 작업이 남아 있어도 더 이상 수행하지 않고 종료한다.
 * 메인(부모) 스레드는 종료된 일꾼 스레드와 조인한 후에 스레드풀에 할당된 자원을 반납한다.
 * 스레드를 종료시키기 위해 철회를 생각할 수 있으나 바람직하지 않다.
 * 락을 소유한 스레드를 중간에 철회하면 교착상태가 발생하기 쉽기 때문이다.
 * 종료가 완료되면 POOL_SUCCESS를 리턴한다.
 */
int pthread_pool_shutdown(pthread_pool_t *pool, int how)
{
    // how가 POOL_DISCARD 또는 POOL_COMPLETE에 해당하지 않으면 FAIL
    if(how != POOL_DISCARD && how != POOL_COMPLETE)
        return POOL_FAIL;

    /*
     * how에 따른 state는 다음과 같음
     *      how == POOL_DISCARD : OFF
     *      how == POOL_COMPLETE : EXIT
     * pool을 종료했으므로 대기중인 모든 공유 변수를 깨움
     */
    pthread_pool_update_state(pool, how == POOL_DISCARD ? OFF : EXIT);
    pthread_cond_broadcast(&pool->full);
    pthread_cond_broadcast(&pool->empty);

    // 모든 스레드가 종료하기를 기다림
    for(int i = 0; i< pool->bee_size; ++i) {
        pthread_join(pool->bee[i], NULL);
    }

    /* 사용한 대기열 및 스레드id 배열 공간을 release */
    free(pool->q);
    free(pool->bee);

    return POOL_SUCCESS;
}
