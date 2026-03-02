export interface SignalingDriver {
  on(namespace: string[], handler: (data: any) => void): void;
  off(namespace: string[], handler: (data: any) => void): void;
  emit(namespace: string[], data: any): void;
}
