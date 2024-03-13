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
import { Contract, JsonRpcProvider, parseUnits, keccak256 } from 'ethers';

import { CHAIN_ID } from '../../common/constants';
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
          const result = await originalMethod.apply(this, args);
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
    throw new NotFoundError('No files to payout found');
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


export const getWalletAddress = async (): Promise<string> => {
  const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION });
  const res = await lambdaClient.send(new InvokeCommand({
    FunctionName: process.env.KMS_LAMBDA_NAME,
    InvocationType: 'RequestResponse',
    Payload: JSON.stringify({
      operation: 'status',
    }),
  }));

  const parsedEthChecksumResp: EthChecksumResponse = JSON.parse(Buffer.from(res.Payload).toString());

  return parsedEthChecksumResp.eth_checksum_address;
};


export const handleChunkPayout = async (
  payoutsInfo: PayoutsInfo,
  payoutChunk: PayoutChunk,
  chunkNum: number,
) => {
  const ethChecksumAddress = await getWalletAddress();
  const provider = new JsonRpcProvider(process.env.INFURA_POLYGON_MUMBAI);
  const escrowContract = new Contract(payoutsInfo.escrowAddress, EscrowAbi, provider);
  const nonce = await provider.getTransactionCount(ethChecksumAddress);
  const feeData = await provider.getFeeData();
  const addresses: Array<string> = [];
  const amounts: Array<number> = [];

  for (let payoutItem of payoutChunk) {
    addresses.push(payoutItem[0]);
    amounts.push(Number(payoutChunk[1]) * 10 ** 18);
  }

  const bulkPayoutGasEstimationFn = await escrowContract.getFunction('bulkPayOut')
    .populateTransaction(
      addresses,
      amounts,
      payoutsInfo.finalResultsUrl,
      payoutsInfo.finalResultsHash,
      chunkNum,
      { from: ethChecksumAddress },
    );
  const bulkPayoutFn = await escrowContract.getFunction('bulkPayOut')
    .populateTransaction(
      addresses,
      amounts,
      payoutsInfo.finalResultsUrl,
      payoutsInfo.finalResultsHash,
      chunkNum,
    );
  const estimatedGas = await provider.estimateGas(bulkPayoutGasEstimationFn);

  try {
    const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION });
    const bulkPayoutSignedRes = await lambdaClient.send(new InvokeCommand({
      FunctionName: process.env.KMS_LAMBDA_NAME,
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify({
        operation: 'sign_bulk_payout',
        value: 0,
        maxFeePerGas: (feeData.gasPrice + BigInt(30)).toString(),
        maxPriorityFeePerGas: feeData.gasPrice.toString(),
        chainId: CHAIN_ID.POLYGON_MUMBAI,
        nonce,
        gas: (estimatedGas * BigInt(110) / BigInt(100)).toString(),
        to: payoutsInfo.escrowAddress,
        data: bulkPayoutFn.data,
      }),
    }));

    const bulkPayoutSigned: BulkPayoutSigned = JSON.parse(Buffer.from(bulkPayoutSignedRes.Payload).toString());
    const tx = await provider.broadcastTransaction(bulkPayoutSigned.signed_tx);

    await tx.wait(1, 2 * 60 * 1000);
  } catch(err) {

  }
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

  async handleChunkPayout() {
    const ethChecksumAddress = await this.getWalletAddress();
  //   const escrowContract = new Contract(payoutsInfo.escrowAddress, EscrowAbi, provider);
  // const nonce = await provider.getTransactionCount(ethChecksumAddress);
  // const feeData = await provider.getFeeData();
  // const addresses: Array<string> = [];
  // const amounts: Array<number> = [];

  // for (let payoutItem of payoutChunk) {
  //   addresses.push(payoutItem[0]);
  //   amounts.push(Number(payoutChunk[1]) * 10 ** 18);
  // }
  }

  @retry(1, 2, [])
  async handleTransactionWithRetry() {

  }
}
