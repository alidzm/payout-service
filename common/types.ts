type ArrayLengthMutationKeys = 'splice' | 'push' | 'pop' | 'shift' |  'unshift';
type FixedLengthArray<T, L extends number, TObj = [T, ...Array<T>]> =
  Pick<TObj, Exclude<keyof TObj, ArrayLengthMutationKeys>>
  & {
    readonly length: L 
    [ I : number ] : T
    [Symbol.iterator]: () => IterableIterator<T>   
  };

export type Metadata = {
  escrow_address: string;
  file_name: string;
  final_results_url: string;
  final_results_hash: string;
};

export type PayoutChunk = Array<FixedLengthArray<string, 2>>;

export type Payouts = Record<string, PayoutChunk>;

export type PayoutsInfo = {
  escrowAddress: string;
  payouts: Payouts;
  metadataKey: string;
  payoutsKey: string;
  finalResultsHash: string;
  finalResultsUrl: string;
};

export type EthChecksumResponse = {
  eth_checksum_address: string;
};

export type BulkPayoutSigned = {
  signed_tx: string;
};
