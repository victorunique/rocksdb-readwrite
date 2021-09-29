package org.example.rocksdbrw;

import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

import java.util.Collection;

public class CustomRocksDBOptionsFactory extends DefaultConfigurableOptionsFactory {
    private static final long serialVersionUID = 1L;

    public boolean enableBloomFilter;
    public boolean indexFilterInCache;
    public boolean aggressiveCompaction;

    public CustomRocksDBOptionsFactory(boolean enableBloomFilter, boolean indexFilterInCache, boolean aggressiveCompaction) {
        this.enableBloomFilter = enableBloomFilter;
        this.indexFilterInCache = indexFilterInCache;
        this.aggressiveCompaction = aggressiveCompaction;
    }

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        currentOptions = super.createDBOptions(currentOptions, handlesToClose);
        currentOptions.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);

        Statistics stats = currentOptions.statistics();
        if (stats==null) {
            stats = new Statistics();
        }
        stats.setStatsLevel(StatsLevel.ALL);

        currentOptions.setStatistics(stats);
        currentOptions.setStatsDumpPeriodSec(60);
        currentOptions.setMaxBackgroundJobs(8);
        return currentOptions;
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        currentOptions = super.createColumnOptions(currentOptions, handlesToClose);
        BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) currentOptions.tableFormatConfig();
        if (this.enableBloomFilter) {
            BloomFilter bloomFilter = new BloomFilter(10, true);
            handlesToClose.add(bloomFilter);
            tableConfig.setFilter(bloomFilter);
        }

        tableConfig.setCacheIndexAndFilterBlocks(this.indexFilterInCache);
        tableConfig.setCacheIndexAndFilterBlocksWithHighPriority(this.indexFilterInCache);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(this.indexFilterInCache);

        currentOptions.setTableFormatConfig(tableConfig);
        if ( this.aggressiveCompaction) {
            currentOptions.setLevel0FileNumCompactionTrigger(1);
        }

        return currentOptions;
    }

}
