package za.co.absa.spline.example.batch;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class HdfsHelper {
    public static void deleteDirectoryRecursively(String path) throws IOException {
        Path directory = Paths.get(path);
        if (directory.toFile().isDirectory()) {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    Files.delete(file); // this will work because it's always a File
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir); //this will work because Files in the directory are already deleted
                    return FileVisitResult.CONTINUE;
                }
            });
        } else {
            throw new IOException(path + "is not a directory");
        }
    }
}
