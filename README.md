# Task Executor Service 

┌─────────────┐
│  Submitter  │ (Multiple threads can submit concurrently)
└──────┬──────┘
       │ submitTask()
       ▼
┌─────────────────────┐
│   Task Queue        │ (LinkedBlockingQueue - maintains order)
│   (FIFO ordered)    │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  Dispatcher Thread  │ (Single thread ensures order)
│  - Check group lock │
│  - Submit to pool   │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  Thread Pool        │ (Fixed size - controls concurrency)
│  ExecutorService    │
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│  Task Execution     │ (CompletableFuture manages async results)
│  + Group Release    │
└─────────────────────┘


<img width="1402" height="1001" alt="Screenshot 2025-10-15 at 11 47 57 PM" src="https://github.com/user-attachments/assets/32d0900c-81ac-4983-8aec-5de1f472756d" />
