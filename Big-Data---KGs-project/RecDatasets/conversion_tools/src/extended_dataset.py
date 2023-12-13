import bz2
import csv
import json
import operator
import os
import re
import time
from datetime import datetime

import numpy as np
import pandas as pd
from tqdm import tqdm

from src.base_dataset import BaseDataset
from src.cosmetics import CosmeticsDataset



class LFM1bDataset(BaseDataset):
    def __init__(self, input_path, output_path, interaction_type, duplicate_removal):
        super(LFM1bDataset, self).__init__(input_path, output_path)
        self.input_path = input_path
        self.output_path = output_path

        self.duplicate_removal = duplicate_removal  # merge repeat interactions if 'duplicate_removal' is True
        self.interaction_type = interaction_type  # artists, albums, tracks
        self.dataset_name = 'lfm1b-' + self.interaction_type

        # input file
        self.inter_file = os.path.join(self.input_path, 'LFM-1b_LEs.txt')
        self.item_file = os.path.join(self.input_path, 'LFM-1b_' + self.interaction_type + '.txt')
        self.user_file = os.path.join(self.input_path, 'LFM-1b_users.txt')
        self.user_file_add = os.path.join(self.input_path, 'LFM-1b_users_additional.txt')

        self.sep = '\t'

        # output file
        self.output_inter_file, self.output_item_file, self.output_user_file = self.get_output_files()

        # selected feature fields
        if self.duplicate_removal == True:
            self.inter_fields = {0: 'user_id:token',
                                 1: self.interaction_type + '_id:token',
                                 2: 'timestamp:float',
                                 3: 'num_repeat:float'
                                 }
        else:
            self.inter_fields = {0: 'user_id:token',
                                 1: self.interaction_type + '_id:token',
                                 2: 'timestamp:float'
                                 }

        if self.interaction_type == 'artists':
            self.item_fields = {0: self.interaction_type + '_id:token',
                                1: 'name:token_seq'
                                }
        else:
            self.item_fields = {0: self.interaction_type + '_id:token',
                                1: 'name:token_seq',
                                2: 'artists_id:token'
                                }

        self.user_fields = {0: 'user_id:token',
                            1: 'country:token',
                            2: 'age:float',
                            3: 'gender:token',
                            4: 'playcount:float',
                            5: 'registered_timestamp:float',
                            6: 'novelty_artist_avg_month:float',
                            7: 'novelty_artist_avg_6months:float',
                            8: 'novelty_artist_avg_year:float',
                            9: 'mainstreaminess_avg_month:float',
                            10: 'mainstreaminess_avg_6months:float',
                            11: 'mainstreaminess_avg_year:float',
                            12: 'mainstreaminess_global:float',
                            13: 'cnt_listeningevents:float',
                            14: 'cnt_distinct_tracks:float',
                            15: 'cnt_distinct_artists:float',
                            16: 'cnt_listeningevents_per_week:float',
                            17: 'relative_le_per_weekday1:float',
                            18: 'relative_le_per_weekday2:float',
                            19: 'relative_le_per_weekday3:float',
                            20: 'relative_le_per_weekday4:float',
                            21: 'relative_le_per_weekday5:float',
                            22: 'relative_le_per_weekday6:float',
                            23: 'relative_le_per_weekday7:float',
                            24: 'relative le per hour0:float',
                            25: 'relative le per hour1:float',
                            26: 'relative le per hour2:float',
                            27: 'relative le per hour3:float',
                            28: 'relative le per hour4:float',
                            29: 'relative le per hour5:float',
                            30: 'relative le per hour6:float',
                            31: 'relative le per hour7:float',
                            32: 'relative le per hour8:float',
                            33: 'relative le per hour9:float',
                            34: 'relative le per hour10:float',
                            35: 'relative le per hour11:float',
                            36: 'relative le per hour12:float',
                            37: 'relative le per hour13:float',
                            38: 'relative le per hour14:float',
                            39: 'relative le per hour15:float',
                            40: 'relative le per hour16:float',
                            41: 'relative le per hour17:float',
                            42: 'relative le per hour18:float',
                            43: 'relative le per hour19:float',
                            44: 'relative le per hour20:float',
                            45: 'relative le per hour21:float',
                            46: 'relative le per hour22:float',
                            47: 'relative le per hour23:float'
                            }

    def convert_inter(self):
        fout = open(self.output_inter_file, 'w')
        fout.write('\t'.join([self.inter_fields[i] for i in range(len(self.inter_fields))]) + '\n')

        if self.duplicate_removal == True:
            self.run_duplicate_removal(fout)
        else:
            with open(self.inter_file, 'r', encoding='utf-8', errors='ignore') as f:
                line = f.readline()
                while True:
                    if not line:
                        break

                    line = line.strip().split('\t')
                    userid, artistid, albumid, trackid, timestamp = line[0], line[1], line[2], line[3], line[4]
                    if self.interaction_type == 'artists':
                        itemid = artistid
                    elif self.interaction_type == 'albums':
                        itemid = albumid
                    else:
                        itemid = trackid
                    fout.write(str(userid) + '\t' + str(itemid) + '\t' + str(timestamp) + '\n')
                    line = f.readline()

        print(self.output_inter_file + ' is done!')
        fout.close()

    def convert_item(self):
        fout = open(self.output_item_file, 'w')
        fout.write('\t'.join([self.item_fields[i] for i in range(len(self.item_fields))]) + '\n')

        cnt_row = 0
        dict_all_items = {}
        with open(self.item_file, 'r', encoding='utf-8', errors='ignore') as f:
            line = f.readline()
            while True:
                if not line:
                    break
                fout.write(line)
                line = f.readline()
        print(self.output_item_file + ' is done!')
        fout.close()

    def convert_user(self):
        fout = open(self.output_user_file, 'w')
        fout.write('\t'.join([self.user_fields[i] for i in range(len(self.user_fields))]) + '\n')

        with open(self.user_file, 'r', encoding='utf-8', errors='ignore') as f1:
            with open(self.user_file_add, 'r') as f2:
                line1 = f1.readline()
                line2 = f2.readline()
                line1 = f1.readline()
                line2 = f2.readline()
                while True:
                    if not line1 or not line2:
                        break
                    line1 = line1.strip()
                    line2 = line2.strip().replace('?', '')
                    line2 = line2.split('\t')
                    fout.write(line1 + '\t')
                    fout.write('\t'.join([line2[i] for i in range(1, len(line2))]) + '\n')
                    line1 = f1.readline()
                    line2 = f2.readline()
        print(self.output_user_file + ' is done!')
        fout.close()
      

    

       
    def run_duplicate_removal(self, fout):
        a_user = {}
        pre_userid = '31435741'
        user_order = []
        with open(self.inter_file, 'r', encoding='utf-8', errors='ignore') as f:
            line = f.readline()
            while True:
                if not line:
                    if pre_userid not in user_order:
                        user_order.append(pre_userid)
                    for userid in user_order:
                        for key, value in a_user[userid].items():
                            fout.write(
                                str(userid) + '\t' + str(key) + '\t' + str(value[0]) + '\t' + str(value[1]) + '\n')
                    break
                line = line.strip().split('\t')
                userid, artistid, albumid, trackid, timestamp = line[0], line[1], line[2], line[3], line[4]
                if self.interaction_type == 'artists':
                    itemid = artistid
                elif self.interaction_type == 'albums':
                    itemid = albumid
                else:
                    itemid = trackid

                if userid not in a_user.keys():
                    a_user[userid] = {}
                if itemid not in a_user[userid].keys():
                    a_user[userid][itemid] = [timestamp, 1]
                else:
                    a_user[userid][itemid][1] += 1

                if userid != pre_userid:
                    if pre_userid not in user_order:
                        user_order.append(pre_userid)
                    pre_userid = userid
                line = f.readline()
 