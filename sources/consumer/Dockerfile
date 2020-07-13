FROM storm

COPY watcher/ ./watcher

CMD ["storm", "jar", "watcher/target/watcher-1.0-SNAPSHOT.jar", "com.btc.Main"]