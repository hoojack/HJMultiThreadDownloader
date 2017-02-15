//
//  HJMultiThreadDownloader.h
//
//  Created by hujianhua on 2017/1/13.
//  Copyright © 2017. All rights reserved.
//

#import <Foundation/Foundation.h>

typedef void (^HJDownloaderItemProgressHandler)(int64_t current, int64_t total);
typedef void (^HJDownloaderProgressHandler)(NSInteger index,  int64_t current, int64_t total);
typedef void (^HJDownloaderCompletionBlock)();

@interface HJDownloaderItem : NSOperation

@property (nonatomic, copy) NSString* url;
@property (nonatomic, copy) NSString* destDir;
@property (nonatomic, copy, readonly) NSString* filePath;
@property (nonatomic, copy) HJDownloaderItemProgressHandler progressHandler;

@end

/**************************************************/

@interface HJMultiThreadDownloader : NSObject

@property (nonatomic, assign) NSUInteger maxConcurentCount;
@property (nonatomic, copy) NSString* destDir;

+ (instancetype)shareInstance;

- (void)downloadWithItems:(NSArray<HJDownloaderItem*>*)items completion:(HJDownloaderCompletionBlock)completion;

- (void)cancel;

@end
