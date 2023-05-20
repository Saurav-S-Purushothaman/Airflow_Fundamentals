def bubble_sort(arr):
    n  = len(arr)
    sorted =True
    while sorted:
        sorted = False
        for i in range(n-1):
            if arr[i] > arr[i+1]:
                sorted = True
                arr[i], arr[i+1] = arr[i+1], arr[i]


                