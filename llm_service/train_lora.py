"""
NL2SQL LoRA Training Pipeline
Fine-tunes Qwen2.5-7B-Instruct for natural language to SQL conversion using PEFT LoRA.
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
import math

import torch
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    TrainingArguments,
    DataCollatorForLanguageModeling,
    EarlyStoppingCallback
)
from peft import (
    LoraConfig,
    get_peft_model,
    TaskType,
    PeftModel
)
from datasets import Dataset
from trl import SFTTrainer, SFTConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Model configuration
DEFAULT_MODEL = "Qwen/Qwen2.5-7B-Instruct"
FALLBACK_MODEL = "Qwen/Qwen2.5-3B-Instruct"

# LoRA configuration
LORA_CONFIG = {
    "r": 16,
    "lora_alpha": 32,
    "lora_dropout": 0.05,
    "target_modules": [
        "q_proj", "k_proj", "v_proj", "o_proj",
        "gate_proj", "up_proj", "down_proj"
    ],
    "bias": "none",
    "task_type": TaskType.CAUSAL_LM
}

# Training hyperparameters
DEFAULT_TRAINING_ARGS = {
    "per_device_train_batch_size": 2,
    "per_device_eval_batch_size": 2,
    "gradient_accumulation_steps": 4,
    "learning_rate": 2e-4,
    "num_train_epochs": 3,
    "max_seq_length": 2048,
    "warmup_ratio": 0.1,
    "logging_steps": 10,
    "save_steps": 100,
    "eval_steps": 100,
    "save_total_limit": 2,
    "load_best_model_at_end": True,
    "metric_for_best_model": "eval_loss",
    "greater_is_better": False,
}


class NL2SQLTrainer:
    """LoRA trainer for NL2SQL model."""
    
    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        output_dir: str = "./output",
        use_8bit: bool = True,
        use_4bit: bool = False
    ):
        self.model_name = model_name
        self.output_dir = output_dir
        self.use_8bit = use_8bit
        self.use_4bit = use_4bit
        self.model = None
        self.tokenizer = None
        self.trainer = None
        
    def load_model_and_tokenizer(self) -> None:
        """Load the base model and tokenizer."""
        logger.info(f"Loading model: {self.model_name}")
        
        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.model_name,
            trust_remote_code=True,
            padding_side="right"
        )
        
        # Set padding token
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        
        # Load model with quantization
        load_kwargs = {
            "trust_remote_code": True,
            "torch_dtype": torch.float16,
            "device_map": "auto"
        }
        
        if self.use_4bit:
            from transformers import BitsAndBytesConfig
            load_kwargs["quantization_config"] = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_compute_dtype=torch.float16,
                bnb_4bit_use_double_quant=True,
                bnb_4bit_quant_type="nf4"
            )
        elif self.use_8bit:
            load_kwargs["load_in_8bit"] = True
        
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_name,
            **load_kwargs
        )
        
        # Configure tokenizer
        self.tokenizer.model_max_length = 2048
        
        logger.info("Model and tokenizer loaded successfully")
        
    def prepare_lora_model(self) -> PeftModel:
        """Apply LoRA to the model."""
        logger.info("Applying LoRA configuration")
        
        lora_config = LoraConfig(
            r=LORA_CONFIG["r"],
            lora_alpha=LORA_CONFIG["lora_alpha"],
            lora_dropout=LORA_CONFIG["lora_dropout"],
            target_modules=LORA_CONFIG["target_modules"],
            bias=LORA_CONFIG["bias"],
            task_type=LORA_CONFIG["task_type"]
        )
        
        peft_model = get_peft_model(self.model, lora_config)
        
        # Print trainable parameters
        peft_model.print_trainable_parameters()
        
        return peft_model
    
    def format_dataset_for_sft(self, dataset: Dataset) -> Dataset:
        """Format dataset for SFT training with chat template."""
        
        def format_example(example: Dict) -> Dict:
            """Format a single example for training."""
            messages = []
            
            # System message
            messages.append({
                "role": "system",
                "content": "You are a dialect-aware NL2SQL generator. Output ONLY valid JSON."
            })
            
            # User message with schema
            user_content = f"""Schema: {example.get('schema_summary', '')}

Relevant Schema: {example.get('retrieved_schema_snippet', '')}

Question: {example.get('question', '')}

Dialect: {example.get('dialect', 'postgres')}

Output only JSON:"""
            
            messages.append({
                "role": "user",
                "content": user_content
            })
            
            # Assistant message (the SQL output as JSON)
            output_json = {
                "sql_query": example.get('sql', ''),
                "dialect": example.get('dialect', 'postgres'),
                "confidence_score": 0.95,
                "referenced_tables": example.get('referenced_tables', []),
                "needs_clarification": False,
                "clarification_question": None
            }
            
            messages.append({
                "role": "assistant",
                "content": json.dumps(output_json, ensure_ascii=False)
            })
            
            return {"messages": messages}
        
        # Apply formatting
        formatted_dataset = dataset.map(
            format_example,
            remove_columns=dataset.column_names
        )
        
        return formatted_dataset
    
    def prepare_dataset(
        self,
        data_path: str,
        test_size: float = 0.1
    ) -> tuple:
        """Load and prepare the training dataset."""
        logger.info(f"Loading dataset from {data_path}")
        
        # Load data
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {len(data)} examples")
        
        # Convert to dataset
        dataset = Dataset.from_list(data)
        
        # Split train/test
        split = dataset.train_test_split(test_size=test_size, seed=42)
        train_dataset = split['train']
        test_dataset = split['test']
        
        # Format for SFT
        train_dataset = self.format_dataset_for_sft(train_dataset)
        test_dataset = self.format_dataset_for_sft(test_dataset)
        
        logger.info(f"Train size: {len(train_dataset)}, Test size: {len(test_dataset)}")
        
        return train_dataset, test_dataset
    
    def train(
        self,
        train_data_path: str,
        **training_kwargs
    ) -> None:
        """Run the training process."""
        # Load model and tokenizer
        self.load_model_and_tokenizer()
        
        # Prepare LoRA model
        peft_model = self.prepare_lora_model()
        
        # Prepare datasets
        train_dataset, eval_dataset = self.prepare_dataset(train_data_path)
        
        # Merge training args with defaults
        training_args = {**DEFAULT_TRAINING_ARGS, **training_kwargs}
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Setup SFT configuration
        sft_config = SFTConfig(
            output_dir=self.output_dir,
            per_device_train_batch_size=training_args["per_device_train_batch_size"],
            per_device_eval_batch_size=training_args["per_device_eval_batch_size"],
            gradient_accumulation_steps=training_args["gradient_accumulation_steps"],
            learning_rate=training_args["learning_rate"],
            num_train_epochs=training_args["num_train_epochs"],
            max_seq_length=training_args["max_seq_length"],
            warmup_ratio=training_args["warmup_ratio"],
            logging_steps=training_args["logging_steps"],
            save_steps=training_args["save_steps"],
            eval_steps=training_args["eval_steps"],
            save_total_limit=training_args["save_total_limit"],
            load_best_model_at_end=training_args["load_best_model_at_end"],
            metric_for_best_model=training_args["metric_for_best_model"],
            greater_is_better=training_args["greater_is_better"],
            evaluation_strategy="steps",
            save_strategy="steps",
            logging_dir=f"{self.output_dir}/logs",
            run_name="nl2sql-lora",
            report_to=["tensorboard"],
            bf16=True,  # Use bf16 for efficiency
            gradient_checkpointing=True,  # Save memory
            ddp_find_unused_parameters=False,
        )
        
        # Initialize trainer
        self.trainer = SFTTrainer(
            model=peft_model,
            args=sft_config,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            formatting_func=lambda x: x["messages"],
            data_collator=DataCollatorForLanguageModeling(
                tokenizer=self.tokenizer,
                mlm=False  # Causal LM, not MLM
            ),
            callbacks=[EarlyStoppingCallback(early_stopping_patience=3)]
        )
        
        # Train
        logger.info("Starting training...")
        self.trainer.train()
        
        # Save the final model
        logger.info("Saving final model...")
        self.trainer.save_model(f"{self.output_dir}/final")
        self.tokenizer.save_pretrained(f"{self.output_dir}/final")
        
        logger.info("Training complete!")
    
    def merge_and_save(self, output_path: str = None) -> None:
        """Merge LoRA weights with base model and save."""
        if output_path is None:
            output_path = f"{self.output_dir}/merged"
        
        logger.info("Merging LoRA weights with base model...")
        
        # Load base model
        base_model = AutoModelForCausalLM.from_pretrained(
            self.model_name,
            trust_remote_code=True,
            torch_dtype=torch.float16,
            device_map="auto"
        )
        
        # Load PEFT model
        peft_model = PeftModel.from_pretrained(
            base_model,
            f"{self.output_dir}/final"
        )
        
        # Merge and save
        merged_model = peft_model.merge_and_unload()
        merged_model.save_pretrained(output_path)
        self.tokenizer.save_pretrained(output_path)
        
        logger.info(f"Merged model saved to {output_path}")


def check_model_availability(model_name: str) -> bool:
    """Check if the model is available locally or can be downloaded."""
    from huggingface_hub import hf_hub_download
    
    try:
        # Try to get model config
        hf_hub_download(repo_id=model_name, filename="config.json", local_dir_only=True)
        return True
    except Exception as e:
        logger.warning(f"Model {model_name} not available: {e}")
        return False


def main():
    """Main training entry point."""
    parser = argparse.ArgumentParser(description="NL2SQL LoRA Training")
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_MODEL,
        help="Base model to fine-tune"
    )
    parser.add_argument(
        "--data",
        type=str,
        required=True,
        help="Path to training data JSON file"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./output",
        help="Output directory for trained model"
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=3,
        help="Number of training epochs"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=2,
        help="Per-device batch size"
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=2e-4,
        help="Learning rate"
    )
    parser.add_argument(
        "--use-8bit",
        action="store_true",
        help="Use 8-bit quantization"
    )
    parser.add_argument(
        "--use-4bit",
        action="store_true",
        help="Use 4-bit quantization"
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Merge LoRA weights after training"
    )
    
    args = parser.parse_args()
    
    # Check model availability
    if not check_model_availability(args.model):
        logger.warning(f"Model {args.model} not found. Will attempt to download.")
    
    # Create trainer
    trainer = NL2SQLTrainer(
        model_name=args.model,
        output_dir=args.output,
        use_8bit=args.use_8bit,
        use_4bit=args.use_4bit
    )
    
    # Train
    trainer.train(
        train_data_path=args.data,
        num_train_epochs=args.epochs,
        per_device_train_batch_size=args.batch_size,
        learning_rate=args.learning_rate
    )
    
    # Optionally merge
    if args.merge:
        trainer.merge_and_save()


if __name__ == "__main__":
    main()