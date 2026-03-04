import torch

def run_agent():
    # Targeted for Proxmox 1660 Ti
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Investigator agent starting on {device}...")

if __name__ == "__main__":
    run_agent()
