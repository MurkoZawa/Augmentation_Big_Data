# @Time   : 2020/9/18
# @Author : Shanlei Mu
# @Email  : slmu@ruc.edu.cn

import argparse
import importlib
import traceback
from src.utils import dataset2class, click_dataset, multiple_dataset, multiple_item_features


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--dataset', type=str, default='ml-1m')
        parser.add_argument('--input_path', type=str, default=None)
        parser.add_argument('--output_path', type=str, default=None)
        parser.add_argument('--interaction_type', type=str, default=None)
        parser.add_argument('--duplicate_removal', action='store_true')
        parser.add_argument('--item_feature_name', type=str, default='none')
        parser.add_argument('--convert_inter', action='store_true')
        parser.add_argument('--convert_item', action='store_true')
        parser.add_argument('--convert_user', action='store_true')
        args = parser.parse_args()

        assert args.input_path is not None, 'input_path can not be None, please specify the input_path'
        assert args.output_path is not None, 'output_path can not be None, please specify the output_path'

        input_args = [args.input_path, args.output_path]
        dataset_class_name = dataset2class[args.dataset.lower()]
        dataset_class = getattr(importlib.import_module('src.extended_dataset'), dataset_class_name)
        if dataset_class_name in multiple_dataset:
            input_args.append(args.interaction_type)
        if dataset_class_name in click_dataset:
            input_args.append(args.duplicate_removal)
        if dataset_class_name in multiple_item_features:
            input_args.append(args.item_feature_name)
        datasets = dataset_class(*input_args)

        if args.convert_inter:
            datasets.convert_inter()
        if args.convert_item:
            datasets.convert_item()
        if args.convert_user:
            datasets.convert_user()

    except UnicodeDecodeError as e:
        print(f"Errore durante la decodifica Unicode: {e}")
        print("Potrebbe essere necessario aggiungere la gestione dell'errore appropriata qui.")
        # Puoi aggiungere ulteriori gestioni dell'errore qui a seconda di come vuoi gestire questa situazione.
    except Exception as e:
        print(f"Errore non gestito: {e}")
        traceback.print_exc()
        # Puoi gestire ulteriori tipi di eccezioni se necessario.
