import torch

def process_batch():
    # Targeted for Mac Pro W6800X Duo
    device = torch.device("mps")
    print(f"Auditor worker starting on {device}...")

if __name__ == "__main__":
    process_batch()
