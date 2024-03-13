import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  DeleteObjectCommand,
  PutObjectCommand,
  _Object,
} from '@aws-sdk/client-s3';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
// @ts-ignore
import EscrowAbi from '@human-protocol/core/abis/Escrow.json';
import { Contract, JsonRpcProvider, parseUnits } from 'ethers';

import { CHAIN_ID, ERROR_CODE } from '../../common/constants';
import {
  BulkPayoutSigned,
  EthChecksumResponse,
  Metadata,
  Payouts,
  PayoutsInfo,
  PayoutChunk,
} from '../../common/types';
import { NotFoundError } from '../../common/errors';


function retry(maxRetries: number, retryDelay: number, retryErrors: string[]) {
  return function(target: any, key: string, descriptor: PropertyDescriptor) {
    const originalMethod = descriptor.value;

    descriptor.value = async function(...args: any[]) {
      let retryCount = 0;
      let lastError: any;

      while (retryCount < maxRetries) {
        try {
          const result = await originalMethod.apply(this, [...args, retryCount]);
          return result; // If the original method succeeds, return its result
        } catch (error) {
          if (retryErrors.includes(error.code)) {
            retryCount++;
            lastError = error;

            console.log(`Retry attempt ${retryCount} for ${key}: ${error.message}`);

            await new Promise((resolve) => setTimeout(resolve, retryDelay)); // Delay before retrying
          } else {
            throw error; // If the error is not in the retryErrors list, propagate it
          }
        }
      }

      throw lastError; // If max retries reached, throw the last error encountered
    }

    return descriptor;
  }
}


export const removeHandledFiles = async (s3Client: S3Client, payoutsInfo: Partial<PayoutsInfo>): Promise<void> => {
  // The function removes files from s3 bucket
  const deleteCommands: Array<DeleteObjectCommand> = [];

  if (payoutsInfo.metadataKey) {
    console.log(`Removing file: ${payoutsInfo.metadataKey}`);
    deleteCommands.push(new DeleteObjectCommand({
      Bucket: process.env.UNPAID_BUCKET_NAME,
      Key: payoutsInfo.metadataKey,
    }));
  }

  if (payoutsInfo.payoutsKey) {
    console.log(`Removing file: ${payoutsInfo.payoutsKey}`);
    deleteCommands.push(new DeleteObjectCommand({
      Bucket: process.env.UNPAID_BUCKET_NAME,
      Key: payoutsInfo.payoutsKey,
    }));
  }

  if (!deleteCommands.length) {
    return;
  }

  await Promise.all(deleteCommands.map((deleteCommand) => s3Client.send(deleteCommand)));
};


export const getAllMetadataFiles = async (s3Client: S3Client): Promise<Array<_Object>> => {
  // Check metadata file and get one with earliest modified date
  const listObjectsCommand = new ListObjectsV2Command({
    Bucket: process.env.UNPAID_BUCKET_NAME,
    Prefix: 'metadata_',
  });

  const { Contents } = await s3Client.send(listObjectsCommand);

  if (!Contents || !Contents.length) {
    return [];
  }

  return Contents.sort((item1, item2) => {
    const date1 = item1.LastModified?.valueOf();
    const date2 = item2.LastModified?.valueOf();

    if (!date1 || !date2) {
      return 0;
    }

    return date1 - date2;
  });
};


export const getPayoutsInfo = async (s3Client: S3Client, fileToHandle: _Object): Promise<PayoutsInfo> => {
  // Try to get metedata file
  const getMetadataObjectCommand = new GetObjectCommand({
    Bucket: process.env.UNPAID_BUCKET_NAME,
    Key: fileToHandle.Key,
  });

  const metadataObjectResp = await s3Client.send(getMetadataObjectCommand);

  if (!metadataObjectResp.Body) {
    try {
      await removeHandledFiles(s3Client, { metadataKey: fileToHandle.Key });
    } catch(err) {
      console.error(err);
    }

    throw new NotFoundError(`File ${fileToHandle.Key} not found`);
  }

  const metadata: Metadata = JSON.parse(await metadataObjectResp.Body.transformToString());

  // Get payouts records based on link to metadata file
  const getPayoutObjectCommand = new GetObjectCommand({
    Bucket: process.env.UNPAID_BUCKET_NAME,
    Key: metadata.file_name,
  });

  const payoutObjectResp = await s3Client.send(getPayoutObjectCommand);

  if (!payoutObjectResp.Body) {
    try {
      await removeHandledFiles(
        s3Client,
        {
          metadataKey: fileToHandle.Key,
          payoutsKey: metadata.file_name,
        },
      );
    } catch(err) {
      console.error(err);
    }

    throw new NotFoundError(`File ${metadata.file_name} not found`);
  }

  const payouts: Payouts = JSON.parse(await payoutObjectResp.Body.transformToString());

  return {
    payouts,
    escrowAddress: metadata.escrow_address,
    metadataKey: fileToHandle.Key,
    payoutsKey: metadata.file_name,
    finalResultsHash: metadata.final_results_hash,
    finalResultsUrl: metadata.final_results_url,
  };
};

export const updatePayouts = async (s3Client: S3Client, payouts: Payouts, fileKey: string): Promise<void> => {
  const putPayoutsCommand = new PutObjectCommand({
    Bucket: process.env.UNPAID_BUCKET_NAME,
    Key: fileKey,
    Body: JSON.stringify(payouts),
  });

  await s3Client.send(putPayoutsCommand);
};


export class Blockchain {
  private lambdaClient: LambdaClient;
  private provider: JsonRpcProvider;

  constructor() {
    this.lambdaClient = new LambdaClient({ region: process.env.AWS_REGION });
    this.provider = new JsonRpcProvider(process.env.INFURA_POLYGON_MUMBAI);
  }

  private async getWalletAddress(): Promise<string> {
    const res = await this.lambdaClient.send(new InvokeCommand({
      FunctionName: process.env.KMS_LAMBDA_NAME,
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify({
        operation: 'status',
      }),
    }));

    const parsedEthChecksumResp: EthChecksumResponse = JSON.parse(Buffer.from(res.Payload).toString());

    return parsedEthChecksumResp.eth_checksum_address;
  }

  async handleChunkPayout(payoutsInfo: PayoutsInfo, payoutChunk: PayoutChunk, chunkNum: number) {
    const ethChecksumAddress = await this.getWalletAddress();
    const escrowContract = new Contract(payoutsInfo.escrowAddress, EscrowAbi, this.provider);
    
    const addresses: Array<string> = [];
    const amounts: Array<bigint> = [];

    for (let payoutItem of payoutChunk) {
      addresses.push(payoutItem[0]);
      amounts.push(parseUnits(payoutItem[1], 'ether'));
    }

    const bulkPayoutArgs: Array<any> = [
      addresses,
      amounts,
      payoutsInfo.finalResultsUrl,
      payoutsInfo.finalResultsHash,
      chunkNum,
    ];

    await this.handleTransactionWithRetry(
      ethChecksumAddress,
      escrowContract,
      payoutsInfo.escrowAddress,
      bulkPayoutArgs,
    );
  }

  @retry(
    3,
    2000,
    [ERROR_CODE.REPLACEMENT_UNDERPRICED, ERROR_CODE.CALL_EXCEPTION, ERROR_CODE.TIMEOUT, ERROR_CODE.TRANSACTION_REPLACED],
  )
  private async handleTransactionWithRetry(
    ethChecksumAddress: string,
    escrowContract: Contract,
    escrowAddress: string,
    bulkPayoutArgs: Array<any>,
    retryCount?: number,
  ) {
    const nonce = await this.provider.getTransactionCount(ethChecksumAddress);
    const feeData = await this.provider.getFeeData();

    const bulkPayoutGasEstimationFn = await escrowContract.getFunction('bulkPayOut')
      .populateTransaction(...bulkPayoutArgs, { from: ethChecksumAddress });
    const bulkPayoutFn = await escrowContract.getFunction('bulkPayOut')
      .populateTransaction(...bulkPayoutArgs);
    
    const estimatedGas = await this.provider.estimateGas(bulkPayoutGasEstimationFn);
    const feeGas = feeData.gasPrice + (retryCount ? feeData.gasPrice * BigInt(retryCount * 10) / BigInt(100) : BigInt(0));

    const bulkPayoutSignedRes = await this.lambdaClient.send(new InvokeCommand({
      FunctionName: process.env.KMS_LAMBDA_NAME,
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify({
        operation: 'sign_bulk_payout',
        value: 0,
        maxFeePerGas: (feeGas + BigInt(30)).toString(),
        maxPriorityFeePerGas: feeGas.toString(),
        chainId: CHAIN_ID.POLYGON_MUMBAI,
        nonce,
        gas: (estimatedGas * BigInt(110) / BigInt(100)).toString(),
        to: escrowAddress,
        data: bulkPayoutFn.data,
      }),
    }));

    const bulkPayoutSigned: BulkPayoutSigned = JSON.parse(Buffer.from(bulkPayoutSignedRes.Payload).toString());
    const tx = await this.provider.broadcastTransaction(bulkPayoutSigned.signed_tx);
    console.log('Trtansaction: ', tx);

    await tx.wait(1, 2 * 60 * 1000);
  }
}
