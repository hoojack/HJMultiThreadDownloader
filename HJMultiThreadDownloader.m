//
//  HJMultiThreadDownloader.m
//
//  Created by hujianhua on 2017/1/13.
//  Copyright © 2017. All rights reserved.
//


#import "HJMultiThreadDownloader.h"
#import <CommonCrypto/CommonDigest.h>

@interface HJDownloaderFileHandleQueue : NSObject

+ (instancetype)fileHandleForWritingAtPath:(NSString *)path error:(NSError **)error;
- (instancetype)initForWritingAtPath:(NSString *)path error:(NSError **)error;

- (BOOL)isValid;
- (void)safeFileHandle:(void(^)(NSFileHandle* fileHandle))block;

@end

@interface HJDownloaderFileHandleQueue()

@property (nonatomic, strong) NSFileHandle* fileHandle;
@property (nonatomic, strong) dispatch_queue_t queue;

@end

@implementation HJDownloaderFileHandleQueue

+ (instancetype)fileHandleForWritingAtPath:(NSString *)path error:(NSError **)error
{
    HJDownloaderFileHandleQueue* handle = [[[self class] alloc] initForWritingAtPath:path error:error];
    return handle;
}

- (instancetype)initForWritingAtPath:(NSString *)path error:(NSError **)error
{
    if (self = [super init])
    {
        NSURL* url = [NSURL fileURLWithPath:path];
        _fileHandle = [NSFileHandle fileHandleForWritingToURL:url error:error];
        
        NSString* queueLabel = [NSString stringWithFormat:@"%@.FileHandleQueue", path];
        _queue = dispatch_queue_create(queueLabel.UTF8String, DISPATCH_QUEUE_SERIAL);
    }
    
    return self;
}

- (void)dealloc
{
    if (_fileHandle != nil)
    {
        [_fileHandle closeFile];
        _fileHandle = nil;
    }
}

- (BOOL)isValid
{
    return self.fileHandle != nil;
}

- (void)safeFileHandle:(void (^)(NSFileHandle *))block
{
    dispatch_sync(self.queue, ^
    {
        if (block != nil)
        {
            block(self.fileHandle);
        }
    });
}

@end

/**************************HJDownloaderItemFragment*********************************/

@class HJDownloaderItemFragment;
@protocol HJDownloaderItemFragmentDelegate <NSObject>

- (void)onFragmentProgressChanged:(HJDownloaderItemFragment*)item current:(NSUInteger)current total:(NSUInteger)total;

@end

@interface HJDownloaderItemFragment : NSOperation

@property (nonatomic, assign) int64_t startPos;
@property (nonatomic, assign) int64_t endPos;

@property (nonatomic, copy) NSString* url;
@property (nonatomic, copy) NSString* destDir;
@property (nonatomic, weak) HJDownloaderFileHandleQueue* fileHandleQueue;
@property (nonatomic, weak) id<HJDownloaderItemFragmentDelegate> delegate;

@end

@interface HJDownloaderItemFragment()

@property (nonatomic, strong) BBHTTPRequest* request;
@property (nonatomic, assign) BOOL taskFinished;
@property (nonatomic, copy) NSString* fileName;
@property (nonatomic, copy) NSString* fragmentPath;

@end


@implementation HJDownloaderItemFragment

- (instancetype)init
{
    if (self = [super init])
    {
        _fileName = [NSString stringWithFormat:@"%@.tmp", [NSUUID UUID].UUIDString];
    }
    
    return self;
}

- (void)start
{
    [NSThread currentThread].name = @"HJDownloaderItemFragment";
    
    BBHTTPRequest* request = [[BBHTTPRequest alloc] initWithURL:[NSURL URLWithString:self.url] andVerb:@"GET"];
    
    NSString* range = [NSString stringWithFormat:@"bytes=%@-%@", @(self.startPos), self.endPos > 0 ? @(self.endPos) : @""];
    [request setValue:range forHeader:@"Range"];
    [request downloadToFile:self.fragmentPath];
    
    __weak typeof(self) weakSelf = self;
    request.downloadProgressBlock = ^(NSUInteger current, NSUInteger total)
    {
        if (weakSelf.delegate != nil && [weakSelf.delegate respondsToSelector:@selector(onFragmentProgressChanged:current:total:)])
        {
            [weakSelf.delegate onFragmentProgressChanged:weakSelf current:current total:total];
        }
    };
    
    self.request = request;
    
    __block NSUInteger retryCount = 3;
    __block BBHTTPResponse* refResponse = nil;
    __block NSError* refError = nil;
    __block BOOL canceled = NO;
    
    do
    {
        dispatch_semaphore_t semp = dispatch_semaphore_create(0);
        
        void (^finally)(BBHTTPResponse* response, NSError* error) = ^(BBHTTPResponse* response, NSError* error)
        {
            refResponse = response;
            refError = error;
            
            dispatch_semaphore_signal(semp);
        };
        
        [request execute:^(BBHTTPResponse *response)
        {
            finally(response, nil);
        }
        error:^(NSError *error)
        {
            finally(nil, error);
        }
        cancelled:^
        {
            canceled = YES;
            finally(nil, nil);
        }
        finally:nil];
        
        dispatch_semaphore_wait(semp, DISPATCH_TIME_FOREVER);
        
        if (refError == nil)
        {
            break;
        }
        else
        {
            if (canceled)
            {
                break;
            }
            NSLog(@"download fragment failed, retry:%ld.", retryCount);
            retryCount--;
        }
        
    } while (retryCount > 0);
        
    [self downloadFinished:refResponse error:refError];
}

- (void)downloadFinished:(BBHTTPResponse*)response error:(NSError*)error
{
    if (error != nil)
    {
        NSLog(@"download fragment failed:%@.", error);
    }
    else if (response != nil)
    {
        @autoreleasepool
        {
            NSError* error = nil;
            NSData* data = [NSData dataWithContentsOfFile:self.fragmentPath options:NSDataReadingMappedIfSafe error:&error];
            if (data.length > 0 && [self.fileHandleQueue isValid])
            {
                int64_t offset = self.startPos;
                [self.fileHandleQueue safeFileHandle:^(NSFileHandle *fileHandle)
                {
                    [fileHandle seekToFileOffset:offset];
                    [fileHandle writeData:data];
                }];
            }
            else
            {
                NSAssert(0, @"downloadFragment write data failed.");
                NSLog(@"download fragment data error.");
            }
            
            data = nil;
            self.fileHandleQueue = nil;
        }
    }
    
    {
        NSError* error = nil;
        [[NSFileManager defaultManager] removeItemAtPath:self.fragmentPath error:&error];
    }
    
    self.taskFinished = YES;
}

- (NSString *)fragmentPath
{
    if (_fragmentPath == nil)
    {
        _fragmentPath = [_destDir stringByAppendingPathComponent:_fileName];
    }
    return _fragmentPath;
}

- (void)cancel
{
    if (self.isFinished)
    {
        return;
    }
    
    [super cancel];
    [self.request cancel];
    
    if ([[NSFileManager defaultManager] fileExistsAtPath:self.fragmentPath])
    {
        NSError* error = nil;
        [[NSFileManager defaultManager] removeItemAtPath:self.fragmentPath error:&error];
    }
}

- (BOOL)isConcurrent
{
    return YES;
}

- (BOOL)isFinished
{
    return self.taskFinished;
}

- (NSString *)description
{
    return [NSString stringWithFormat:@"fragment-range:%@-%@", @(self.startPos), self.endPos > 0 ? @(self.endPos) : @""];
}

@end


/************************HJDownloaderItem*****************************/

@interface HJDownloaderItem() <HJDownloaderItemFragmentDelegate>

@property (nonatomic, strong) BBHTTPRequest* request;
@property (nonatomic, strong) NSOperationQueue* queue;
@property (nonatomic, assign) BOOL taskFinished;

@property (nonatomic, copy) NSString* fileTempPath;
@property (nonatomic, assign) int64_t fileSize;
@property (nonatomic, assign) int64_t fragmentSize;

@property (nonatomic, strong) NSMutableDictionary* fragmentProgress;

@end

@implementation HJDownloaderItem

- (instancetype)init
{
    if (self = [super init])
    {
        _queue = [[NSOperationQueue alloc] init];
        _queue.maxConcurrentOperationCount = 10;
        _fragmentSize = 10 * 1024 * 1024;
        _fragmentProgress = [NSMutableDictionary dictionary];
    }
    return self;
}

- (void)dealloc
{
    if (_fragmentProgress != nil)
    {
        [_fragmentProgress removeAllObjects];
        _fragmentProgress = nil;
    }
}

- (BOOL)isConcurrent
{
    return YES;
}

- (BOOL)isFinished
{
    return self.taskFinished;
}

- (void)cancel
{
    if (self.isFinished)
    {
        return;
    }
    
    [super cancel];
    [self.request cancel];
    [self.queue cancelAllOperations];
    
    if ([[NSFileManager defaultManager] fileExistsAtPath:self.fileTempPath])
    {
        NSError* error = nil;
        [[NSFileManager defaultManager] removeItemAtPath:self.fileTempPath error:&error];
    }
}

- (void)start
{
    [NSThread currentThread].name = @"HJDownloaderItem";
    
    [self createDownloadFilePath];
    
    dispatch_semaphore_t semp = dispatch_semaphore_create(0);
    
    __block BBHTTPResponse* refResponse = nil;
    __block NSError* refError = nil;
    
    void (^finally)(BBHTTPResponse* response, NSError* error) = ^(BBHTTPResponse* response, NSError* error)
    {
        refResponse = response;
        refError = error;
        
        dispatch_semaphore_signal(semp);
    };
    
    BBHTTPRequest* request = [[BBHTTPRequest alloc] initWithURL:[NSURL URLWithString:self.url] andVerb:@"GET"];
    request.noBody = YES;
    
    [request execute:^(BBHTTPResponse *response)
    {
        finally(response, nil);
        
    } error:^(NSError *error)
    {
        finally(nil, error);
    }
    cancelled:^
    {
        finally(nil, nil);
    }
    finally:nil];
    
    self.request = request;
    
    dispatch_semaphore_wait(semp, DISPATCH_TIME_FOREVER);
    
    [self prepareDownloadFragment:refResponse error:refError];
}

- (void)prepareDownloadFragment:(BBHTTPResponse*)response error:(NSError*) error
{
    if (error != nil)
    {
        NSLog(@"prepareDownloadFragment failed:%@.", error);
    }
    else if (response != nil)
    {
        self.fileSize =  [[response.headers objectForKey:@"Content-Length"] longLongValue];
        if (self.fileSize == 0)
        {
            NSLog(@"downloadFragment invalid fileSize:%lld.", self.fileSize);
        }
        [self downloadFragment:self.fileSize];
    }
    
    self.taskFinished = YES;
}

- (void)downloadFragment:(int64_t)fileSize
{
    NSError* error = nil;
    if (![[NSFileManager defaultManager] fileExistsAtPath:self.fileTempPath])
    {
        NSData* data = [NSData data];
        [[NSFileManager defaultManager] createFileAtPath:self.fileTempPath contents:data attributes:nil];
    }
    
    @autoreleasepool
    {
        HJDownloaderFileHandleQueue* fileHandleQueue = [HJDownloaderFileHandleQueue fileHandleForWritingAtPath:self.fileTempPath error:&error];
        
        if ([fileHandleQueue isValid] && error == nil)
        {
            [fileHandleQueue safeFileHandle:^(NSFileHandle *fileHandle)
            {
                [fileHandle truncateFileAtOffset:fileSize];
            }];
        }
        else
        {
            NSAssert(0, @"downloadFragment createFile failed.");
            NSLog(@"downloadFragment createFile failed:%@.", error);
        }
            
        NSMutableArray* operations = [NSMutableArray array];
        int64_t fragmentSize = self.fragmentSize;
        
        for (int64_t start = 0; start < fileSize;)
        {
            HJDownloaderItemFragment* fragment = [[HJDownloaderItemFragment alloc] init];
            fragment.url = self.url;
            fragment.destDir = self.destDir;
            fragment.fileHandleQueue = fileHandleQueue;
            fragment.delegate = self;
            
            int64_t end = start + fragmentSize + 1;
            if (end < fileSize)
            {
                fragment.startPos = start;
                fragment.endPos = end - 1;
                start = end;
            }
            else
            {
                end = 0;
                fragment.startPos = start;
                fragment.endPos = end;
                start = fileSize;
            }
            
            [operations addObject:fragment];
        }
        
        NSLog(@"downloadFragment:%lld-%@.", fileSize, operations);
        
        [self.queue addOperations:operations waitUntilFinished:YES];
        
        fileHandleQueue = nil;
        
        if ([[NSFileManager defaultManager] fileExistsAtPath:self.fileTempPath])
        {
            NSError* error = nil;
            if ([[NSFileManager defaultManager] fileExistsAtPath:self.filePath])
            {
                [[NSFileManager defaultManager] removeItemAtPath:self.filePath error:&error];
            }
            
            [[NSFileManager defaultManager] moveItemAtPath:self.fileTempPath toPath:self.filePath error:&error];
            
            if (error != nil)
            {
                NSLog(@"HJDownloaderItem move tempfile to destfile error:%@.", error);
            }
        }
    }
}

- (void)createDownloadFilePath
{
    NSString* fileExt = [_url pathExtension];
    
    if (fileExt.length > 0)
    {
        NSUInteger location = [fileExt rangeOfString:@"?"].location;
        if (location != NSNotFound)
        {
            fileExt = [fileExt substringToIndex:location];
        }
    }
    
    NSString* fileName = [NSString stringWithFormat:@"%@%@",
                          [self fileNameMD5:self.url], fileExt.length > 0 ? [NSString stringWithFormat:@".%@", fileExt] : @""];
    
    
    NSString* destDir = _destDir;
    _filePath = [destDir stringByAppendingPathComponent:fileName];
    _fileTempPath = [_filePath stringByAppendingPathExtension:@"tmp"];
}

- (NSString*)fileNameMD5:(NSString*)url
{
    const char* urlstr = [url UTF8String];
    unsigned char r[CC_MD5_DIGEST_LENGTH];
    CC_MD5(urlstr, (CC_LONG)strlen(urlstr), r);
    NSString* fileName = [NSString stringWithFormat:@"%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
                          r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10],
                          r[11], r[12], r[13], r[14], r[15]];
                          
    
    return fileName;
}

#pragma mark - HJDownloaderItemFragmentDelegate
- (void)onFragmentProgressChanged:(HJDownloaderItemFragment *)item current:(NSUInteger)current total:(NSUInteger)total
{
    [self.fragmentProgress setObject:@(current) forKey:item.fileName];
    
    int64_t received = 0;
    int64_t totalSize = self.fileSize;
    
    for (NSNumber* progress in self.fragmentProgress.allValues)
    {
        received += progress.longLongValue;
    }
    
    if (received == totalSize)
    {
        [self.fragmentProgress removeAllObjects];
    }
    
    dispatch_async(dispatch_get_main_queue(), ^
    {
        if (self.progressHandler != nil)
        {
            self.progressHandler(received, totalSize);
        }
    });
}

@end

/************************DVRMultiThreadDownloader*****************************/

@interface HJMultiThreadDownloader()

@property (nonatomic, strong) NSOperationQueue* queue;

@end

@implementation HJMultiThreadDownloader

+ (instancetype)shareInstance
{
    static HJMultiThreadDownloader* sInstance = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        sInstance = [[[self class] alloc] init];
    });
    
    return sInstance;
}

- (instancetype)init
{
    if (self = [super init])
    {
        _queue = [[NSOperationQueue alloc] init];
        
        [self defaultSetting];
    }
    
    return self;
}

- (void)defaultSetting
{
    self.maxConcurentCount = 6;
    self.queue.maxConcurrentOperationCount = self.maxConcurentCount;
    
    self.destDir = [@"~/tmp/downloadDir" stringByExpandingTildeInPath];
    
    [BBHTTPExecutor sharedExecutor].maxParallelRequests = 10;
}

- (void)setDestDir:(NSString *)destDir
{
    _destDir = destDir;
    
    BOOL isDir = YES;
    if (![[NSFileManager defaultManager] fileExistsAtPath:_destDir isDirectory:&isDir] ||
        !isDir)
    {
        NSError* error = nil;
        [[NSFileManager defaultManager] createDirectoryAtPath:_destDir withIntermediateDirectories:YES attributes:nil error:&error];
        
        if (error != nil)
        {
            NSLog(@"create download directory failed.");
        }
    }
}

- (void)downloadWithItems:(NSArray<HJDownloaderItem*>*)items completion:(HJDownloaderCompletionBlock)completion
{
    HJDownloaderCompletionBlock completionBlock = completion;
    
    dispatch_queue_t taskQueue = dispatch_queue_create("HJMultiThreadDownloader.taskQueue", DISPATCH_QUEUE_SERIAL);
    
    __weak typeof(self) weakSelf = self;
    dispatch_async(taskQueue, ^
    {
        for (HJDownloaderItem* item in items)
        {
            item.destDir = weakSelf.destDir;
        }
        
        [weakSelf.queue addOperations:items waitUntilFinished:completionBlock != nil];
        
        if (completionBlock != nil)
        {
            dispatch_async(dispatch_get_main_queue(), ^
            {
                completionBlock(0, 0);
            });
        }
    });
}

- (void)cancel
{
    [self.queue cancelAllOperations];
}

@end
