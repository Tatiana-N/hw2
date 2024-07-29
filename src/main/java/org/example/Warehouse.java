package org.example;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.SynchronousQueue;

@Log4j2
@Getter
public class Warehouse extends Thread {
    
    private final  Queue<Block> storage = new LinkedList<>();
    private final  Queue<Truck> trucks = new LinkedList<>();
    
    public Warehouse(String name) {
        super(name);
    }

    public Warehouse(String name, Collection<Block> initialStorage) {
        this(name);
        storage.addAll(initialStorage);
    }

    @Override
    public void run() {
        Truck truck;
        while (!currentThread().isInterrupted()) {
            truck = getNextArrivedTruck();
            if (truck == null) {
                try {
                    sleep(100);
                } catch (InterruptedException e) {
                    if (currentThread().isInterrupted()) {

                        break;
                    }
                }
                continue;
            }
            synchronized (truck) {
                if (truck.getBlocks().isEmpty()) {
                    loadTruck(truck);
                    truck.notifyAll();
                } else {
                    unloadTruck(truck);
                    truck.notifyAll();
        
                }
            }
        }
        log.info("Warehouse thread interrupted");

    }

    private void loadTruck(Truck truck) {
        log.info("Loading truck {}", truck.getName());
        Collection<Block> blocksToLoad = getFreeBlocks(truck.getCapacity());
        try {
            sleep(blocksToLoad.size());
        } catch (InterruptedException e) {
            log.error("Interrupted while loading truck", e);
        }
        truck.getBlocks().addAll(blocksToLoad);
        log.info("Truck loaded {}", truck.getName());
    }

    private Collection<Block> getFreeBlocks(int maxItems) {
        //TODO необходимо реализовать потокобезопасную логику по получению свободных блоков
        //TODO 1 блок грузится в 1 грузовик, нельзя клонировать блоки во время загрузки
        List<Block> blocks = new ArrayList<>();
        for (int i = 0; i < maxItems; i++) {
            blocks.add(storage.poll());
        }
        return blocks;
    }

    private void returnBlocksToStorage(List<Block> returnedBlocks) {
        //TODO реализовать потокобезопасную логику по возврату блоков на склад
            storage.addAll(returnedBlocks);
    }

    private void unloadTruck(Truck truck) {
        log.info("Unloading truck {}", truck.getName());
        List<Block> arrivedBlocks = truck.getBlocks();
        try {
            sleep(arrivedBlocks.size());
        } catch (InterruptedException e) {
            log.error("Interrupted while unloading truck", e);
        }
        returnBlocksToStorage(arrivedBlocks);
        truck.getBlocks().clear();
        log.info("Truck unloaded {}", truck.getName());
    }

    private Truck getNextArrivedTruck() {
        //TODO необходимо реализовать логику по получению следующего прибывшего грузовика внутри потока склада
        return trucks.poll();
    }


    public void arrive(Truck truck) {
        //TODO необходимо реализовать логику по сообщению потоку склада о том, что грузовик приехал
        //TODO так же дождаться разгрузки блоков, при возврате из этого метода - грузовик покинет склад
        trucks.add(truck);
        try {
            synchronized (truck) {
                truck.wait();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
