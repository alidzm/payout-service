import * as dotenv from 'dotenv';
dotenv.config();

import type { VercelRequest, VercelResponse } from '@vercel/node';
import { S3Client } from '@aws-sdk/client-s3';

import { NotFoundError } from '../../common/errors';
import { PayoutsInfo } from '../../common/types';

import {
  getAllMetadataFiles,
  getPayoutsInfo,
  handleChunkPayout,
  removeHandledFiles,
  updatePayouts,
} from './helpers';

export default async function handler(
  request: VercelRequest,
  response: VercelResponse,
) {
  const s3Client = new S3Client({ region: process.env.AWS_REGION });

  let payoutsInfo: PayoutsInfo;

  const metadataFiles = await getAllMetadataFiles(s3Client);

  for (let metadataFile of metadataFiles) {
    try {
      payoutsInfo = await getPayoutsInfo(s3Client, metadataFile);
    } catch(err) {
      if (err instanceof NotFoundError) {
        response.status(404);
        return response.json({ message: err.message });
      }
  
      throw err;
    }

    const payouts = payoutsInfo.payouts;

    const chunkNums = Object.keys(payouts);
    for (let chunkNum of chunkNums) {
      const payoutChunk = payouts[chunkNum];

      await handleChunkPayout(payoutsInfo, payoutChunk, Number(chunkNum));

      delete payouts[chunkNums[0]];

      await updatePayouts(s3Client, payouts, payoutsInfo.payoutsKey);
    }
  
    // Remove files as no more items for payout
    if (!Object.keys(payouts).length) {
      await removeHandledFiles(
        s3Client,
        {
          metadataKey: payoutsInfo.metadataKey,
          payoutsKey: payoutsInfo.payoutsKey,
        },
      );
    }
  }

  return response.json({ message: 'ok' });
}
