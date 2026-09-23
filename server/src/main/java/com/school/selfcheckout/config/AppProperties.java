package com.school.selfcheckout.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private boolean resetOnStartup = false;
    private int catalogSize = 2000;
    private int stockPerItem = 10000;
    private int lowStockThreshold = 50;
    private Popularity popularity = new Popularity();

    public static class Popularity {
        private int windowSize = 1000;
        private int slideInterval = 500;

        public int getWindowSize() {
            return windowSize;
        }

        public void setWindowSize(int windowSize) {
            this.windowSize = windowSize;
        }

        public int getSlideInterval() {
            return slideInterval;
        }

        public void setSlideInterval(int slideInterval) {
            this.slideInterval = slideInterval;
        }
    }

    public boolean isResetOnStartup() {
        return resetOnStartup;
    }

    public void setResetOnStartup(boolean resetOnStartup) {
        this.resetOnStartup = resetOnStartup;
    }

    public int getCatalogSize() {
        return catalogSize;
    }

    public void setCatalogSize(int catalogSize) {
        this.catalogSize = catalogSize;
    }

    public int getStockPerItem() {
        return stockPerItem;
    }

    public void setStockPerItem(int stockPerItem) {
        this.stockPerItem = stockPerItem;
    }

    public int getLowStockThreshold() {
        return lowStockThreshold;
    }

    public void setLowStockThreshold(int lowStockThreshold) {
        this.lowStockThreshold = lowStockThreshold;
    }

    public Popularity getPopularity() {
        return popularity;
    }

    public void setPopularity(Popularity popularity) {
        this.popularity = popularity;
    }
}
