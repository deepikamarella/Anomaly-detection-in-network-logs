import re
import pandas as pd
import hashlib
import copy
import sys
from datetime import datetime
import os

class Event:
  def __init__(self, eventStr):
        self.eventStr = eventStr
        self.eventId = hashlib.md5(' '.join(eventStr).encode('utf-8')).hexdigest()[0:8]
        self.eventCount = 0
    
class LogParser:
    def __init__(self, log_format, log_lines):
        self.log_lines = log_lines
        self.log_format = log_format
        self.partitions = []
        self.events = []

    def parse(self):
        self.step1()
        self.step2()
        self.step3()
        self.step4()
        self.get_output()
        self.write_event_to_file()

    def step1(self):
        self.partitions = []

        headers, regex = self.generate_logformat_regex(self.log_format)
        lineCount = 1

        for line in self.log_lines:
            match = regex.search(line)
            if match:
                message = [match.group(header) for header in headers]

            
                terms_list = message + [str(lineCount)]
                lineCount += 1

                partition = Partition(1, len(terms_list), terms_list)
                self.partitions.append(partition)

    def step2(self):
     
        for partition in self.partitionsL:

            if not partition.valid:
                continue

            if partition.numOfLogs <= self.para.step2Support:
                continue

            
            if partition.stepNo == 2:
                break

            
            uniqueTokensCountLS = []
            for columnIdx in range(partition.lenOfLogs):
                uniqueTokensCountLS.append(set())

            for logL in partition.logLL:
                for columnIdx in range(partition.lenOfLogs):
                    uniqueTokensCountLS[columnIdx].add(logL[columnIdx])

            
            minColumnIdx = 0
            minColumnCount = len(uniqueTokensCountLS[0])

            for columnIdx in range(partition.lenOfLogs):
                if minColumnCount > len(uniqueTokensCountLS[columnIdx]):
                    minColumnCount = len(uniqueTokensCountLS[columnIdx])
                    minColumnIdx = columnIdx

            
            if minColumnCount == 1:
                continue

            
            logDLL = {}
            for logL in partition.logLL:
                if logL[minColumnIdx] not in logDLL:
                    logDLL[logL[minColumnIdx]] = []
                logDLL[logL[minColumnIdx]].append(logL)

            for key in logDLL:
                if self.para.PST != 0 and 1.0 * len(logDLL[key]) / partition.numOfLogs < self.para.PST:
                    self.partitionsL[0].logLL += logDLL[key]
                    self.partitionsL[0].numOfLogs += len(logDLL[key])
                else:
                    newPartition = Partition(stepNo=2, numOfLogs=len(logDLL[key]), lenOfLogs=partition.lenOfLogs)
                    newPartition.logLL = logDLL[key]
                    self.partitionsL.append(newPartition)

            partition.valid = False

    def step3(self):
        # Implement Step 3 logic
        for partition in self.partitionsL:

            if not partition.valid:
                continue

            if partition.stepNo == 3:
                break

            
            p1, p2 = self.DetermineP1P2(partition)

            if p1 == -1 or p2 == -1:
                continue

            try:

                p1Set = set()
                p2Set = set()
                mapRelation1DS = {}
                mapRelation2DS = {}

                
                for logL in partition.logLL:
                    p1Set.add(logL[p1])
                    p2Set.add(logL[p2])

                    if (logL[p1] == logL[p2]):
                        print ("Warning: p1 may be equal to p2")

                    if logL[p1] not in mapRelation1DS:
                        mapRelation1DS[logL[p1]] = set()
                    mapRelation1DS[logL[p1]].add(logL[p2])

                    if logL[p2] not in mapRelation2DS:
                        mapRelation2DS[logL[p2]] = set()
                    mapRelation2DS[logL[p2]].add(logL[p1])

                
                oneToOneS = set()
                oneToMP1D = {}
                oneToMP2D = {}

                
                for p1Token in p1Set:
                    if len(mapRelation1DS[p1Token]) == 1:
                        if len(mapRelation2DS[list(mapRelation1DS[p1Token])[0]]) == 1:
                            oneToOneS.add(p1Token)

                    else:
                        isOneToM = True

                        for p2Token in mapRelation1DS[p1Token]:
                            if len(mapRelation2DS[p2Token]) != 1:
                                isOneToM = False
                                break
                        if isOneToM:
                            oneToMP1D[p1Token] = 0

                
                for deleteToken in oneToOneS:
                    p1Set.remove(deleteToken)
                    p2Set.remove(list(mapRelation1DS[deleteToken])[0])

                for deleteToken in oneToMP1D:
                    for deleteTokenP2 in mapRelation1DS[deleteToken]:
                        p2Set.remove(deleteTokenP2)
                    p1Set.remove(deleteToken)

                
                for p2Token in p2Set:
                    if len(mapRelation2DS[p2Token]) != 1:
                        isOneToM = True
                        for p1Token in mapRelation2DS[p2Token]:
                            if len(mapRelation1DS[p1Token]) != 1:
                                isOneToM = False
                                break
                        if isOneToM:
                            oneToMP2D[p2Token] = 0

                
                for deleteToken in oneToMP2D:
                    p2Set.remove(deleteToken)
                    for deleteTokenP1 in mapRelation2DS[deleteToken]:
                        p1Set.remove(deleteTokenP1)

                
                for logL in partition.logLL:
                    if logL[p1] in oneToMP1D:
                        oneToMP1D[logL[p1]] += 1

                    if logL[p2] in oneToMP2D:
                        oneToMP2D[logL[p2]] += 1

            except KeyError as er:
                print (er)
                print ('error: ' + str(p1) + '\t' + str(p2))

            newPartitionsD = {}
            if partition.stepNo == 2:
                newPartitionsD["dumpKeyforMMrelationInStep2__"] = Partition(stepNo=3, numOfLogs=0,
                                                                            lenOfLogs=partition.lenOfLogs)
            
            for logL in partition.logLL:
                
                if logL[p1] in oneToOneS:
                    if logL[p1] not in newPartitionsD:
                        newPartitionsD[logL[p1]] = Partition(stepNo=3, numOfLogs=0, lenOfLogs=partition.lenOfLogs)
                    newPartitionsD[logL[p1]].logLL.append(logL)
                    newPartitionsD[logL[p1]].numOfLogs += 1

                
                elif logL[p1] in oneToMP1D:
                    split_rank = self.Get_Rank_Posistion(len(mapRelation1DS[logL[p1]]), oneToMP1D[logL[p1]], True)
                    if split_rank == 1:
                        if logL[p1] not in newPartitionsD:
                            newPartitionsD[logL[p1]] = Partition(stepNo=3, numOfLogs=0, lenOfLogs=partition.lenOfLogs)
                        newPartitionsD[logL[p1]].logLL.append(logL)
                        newPartitionsD[logL[p1]].numOfLogs += 1
                    else:
                        if logL[p2] not in newPartitionsD:
                            newPartitionsD[logL[p2]] = Partition(stepNo=3, numOfLogs=0, lenOfLogs=partition.lenOfLogs)
                        newPartitionsD[logL[p2]].logLL.append(logL)
                        newPartitionsD[logL[p2]].numOfLogs += 1

                
                elif logL[p2] in oneToMP2D:
                    split_rank = self.Get_Rank_Posistion(len(mapRelation2DS[logL[p2]]), oneToMP2D[logL[p2]], False)
                    if split_rank == 1:
                        if logL[p1] not in newPartitionsD:
                            newPartitionsD[logL[p1]] = Partition(stepNo=3, numOfLogs=0, lenOfLogs=partition.lenOfLogs)
                        newPartitionsD[logL[p1]].logLL.append(logL)
                        newPartitionsD[logL[p1]].numOfLogs += 1
                    else:
                        if logL[p2] not in newPartitionsD:
                            newPartitionsD[logL[p2]] = Partition(stepNo=3, numOfLogs=0, lenOfLogs=partition.lenOfLogs)
                        newPartitionsD[logL[p2]].logLL.append(logL)
                        newPartitionsD[logL[p2]].numOfLogs += 1

                
                else:
                    if partition.stepNo == 2:
                        newPartitionsD["dumpKeyforMMrelationInStep2__"].logLL.append(logL)
                        newPartitionsD["dumpKeyforMMrelationInStep2__"].numOfLogs += 1
                    else:
                        if len(p1Set) < len(p2Set):
                            if logL[p1] not in newPartitionsD:
                                newPartitionsD[logL[p1]] = Partition(stepNo=3, numOfLogs=0,
                                                                     lenOfLogs=partition.lenOfLogs)
                            newPartitionsD[logL[p1]].logLL.append(logL)
                            newPartitionsD[logL[p1]].numOfLogs += 1
                        else:
                            if logL[p2] not in newPartitionsD:
                                newPartitionsD[logL[p2]] = Partition(stepNo=3, numOfLogs=0,
                                                                     lenOfLogs=partition.lenOfLogs)
                            newPartitionsD[logL[p2]].logLL.append(logL)
                            newPartitionsD[logL[p2]].numOfLogs += 1

            if "dumpKeyforMMrelationInStep2__" in newPartitionsD and newPartitionsD[
                "dumpKeyforMMrelationInStep2__"].numOfLogs == 0:
                newPartitionsD["dumpKeyforMMrelationInStep2__"].valid = False
            
            for key in newPartitionsD:
                if self.para.PST != 0 and 1.0 * newPartitionsD[key].numOfLogs / partition.numOfLogs < self.para.PST:
                    self.partitionsL[0].logLL += newPartitionsD[key].logLL
                    self.partitionsL[0].numOfLogs += newPartitionsD[key].numOfLogs
                else:
                    self.partitionsL.append(newPartitionsD[key])

            partition.valid = False

    def step4(self):
        # Implement Step 4 logic
        self.partitionsL[0].valid = False
        if self.para.PST == 0 and self.partitionsL[0].numOfLogs != 0:
            event = Event(['Outlier'])
            event.eventCount = self.partitionsL[0].numOfLogs
            self.eventsL.append(event)

            for logL in self.partitionsL[0].logLL:
                logL.append(str(event.eventId))

        for partition in self.partitionsL:
            if not partition.valid:
                continue

            if partition.numOfLogs == 0:
                print (str(partition.stepNo) + '\t')

            uniqueTokensCountLS = []
            for columnIdx in range(partition.lenOfLogs):
                uniqueTokensCountLS.append(set())

            for logL in partition.logLL:
                for columnIdx in range(partition.lenOfLogs):
                    uniqueTokensCountLS[columnIdx].add(logL[columnIdx])

            e = copy.deepcopy(partition.logLL[0])[:partition.lenOfLogs]

            for columnIdx in range(partition.lenOfLogs):
                if len(uniqueTokensCountLS[columnIdx]) == 1:
                    continue
                else:
                    e[columnIdx] = '<*>'

            event = Event(e)
            event.eventCount = partition.numOfLogs

            self.eventsL.append(event)

            for logL in partition.logLL:
                logL.append(str(event.eventId))

    def get_output(self):
        # Get the parsed log lines
        if self.para.PST == 0 and self.partitionsL[0].numOfLogs != 0:
            for logL in self.partitionsL[0].logLL:
                self.output.append(logL[-2:] + logL[:-2])
        for partition in self.partitionsL:
            if not partition.valid:
                continue
            for logL in partition.logLL:
                self.output.append(logL[-2:] + logL[:-2])

    def write_event_to_file(self):
        # Write the events to files
        eventID_template = {event.eventId: ' '.join(event.eventStr) for event in self.eventsL}
        eventList = [[event.eventId, ' '.join(event.eventStr), event.eventCount] for event in self.eventsL]
        eventDf = pd.DataFrame(eventList, columns=['EventID', 'EventTemplate', 'Occurrences'])
        eventDf.to_csv(os.path.join(self.para.savePath, self.logname + '_templates.csv'), index=False)

        self.output.sort(key=lambda x: int(x[0]))
        self.df_log['EventID'] = [str(logL[1]) for logL in self.output]
        self.df_log['EventTemplate'] = [eventID_template[logL[1]] for logL in self.output]
        if self.keep_para:
            self.df_log["ParameterList"] = self.df_log.apply(self.get_parameter_list, axis=1) 
        self.df_log.to_csv(os.path.join(self.para.savePath, self.logname + '_structured.csv'), index=False)

    def Get_Mapping_Position(self, partition, uniqueTokensCountLS):
        p1 = p2 = -1

        numOfUniqueTokensD = {}
        for columnIdx in range(partition.lenOfLogs):
            if len(uniqueTokensCountLS[columnIdx]) not in numOfUniqueTokensD:
                numOfUniqueTokensD[len(uniqueTokensCountLS[columnIdx])] = 0
            numOfUniqueTokensD[len(uniqueTokensCountLS[columnIdx])] += 1

        if partition.stepNo == 2:

            
            maxIdx = secondMaxIdx = -1
            maxCount = secondMaxCount = 0
            for key in numOfUniqueTokensD:
                if key == 1:
                    continue
                if numOfUniqueTokensD[key] > maxCount:
                    secondMaxIdx = maxIdx
                    secondMaxCount = maxCount
                    maxIdx = key
                    maxCount = numOfUniqueTokensD[key]
                elif numOfUniqueTokensD[key] > secondMaxCount and numOfUniqueTokensD[key] != maxCount:
                    secondMaxIdx = key
                    secondMaxCount = numOfUniqueTokensD[key]

            
            if maxCount > 1:
                for columnIdx in range(partition.lenOfLogs):
                    if len(uniqueTokensCountLS[columnIdx]) == maxIdx:
                        if p1 == -1:
                            p1 = columnIdx
                        else:
                            p2 = columnIdx
                            break

            else:
                for columnIdx in range(partition.lenOfLogs):
                    if len(uniqueTokensCountLS[columnIdx]) == maxIdx:
                        p1 = columnIdx
                        break

                for columnIdx in range(partition.lenOfLogs):
                    if len(uniqueTokensCountLS[columnIdx]) == secondMaxIdx:
                        p2 = columnIdx
                        break

            if p1 == -1 or p2 == -1:
                return (-1, -1)
            else:
                return (p1, p2)

        
        else:
            minIdx = secondMinIdx = -1
            minCount = secondMinCount = sys.maxsize
            for key in numOfUniqueTokensD:
                if numOfUniqueTokensD[key] < minCount:
                    secondMinIdx = minIdx
                    secondMinCount = minCount
                    minIdx = key
                    minCount = numOfUniqueTokensD[key]
                elif numOfUniqueTokensD[key] < secondMinCount and numOfUniqueTokensD[key] != minCount:
                    secondMinIdx = key
                    secondMinCount = numOfUniqueTokensD[key]

            for columnIdx in range(len(uniqueTokensCountLS)):
                if numOfUniqueTokensD[len(uniqueTokensCountLS[columnIdx])] == minCount:
                    if p1 == -1:
                        p1 = columnIdx
                        break

            for columnIdx in range(len(uniqueTokensCountLS)):
                if numOfUniqueTokensD[len(uniqueTokensCountLS[columnIdx])] == secondMinCount:
                    p2 = columnIdx
                    break

            return (p1, p2)

    def PrintPartitions(self):
        for idx in range(len(self.partitionsL)):
            print ('Partition {}:(from step {})    Valid:{}'.format(idx, self.partitionsL[idx].stepNo,
                                                                    self.partitionsL[idx].valid))

            for log in self.partitionsL[idx].logLL:
                print (log)

    def PrintEventStats(self):
        for event in self.eventsL:
            if event.eventCount > 1:
                print (str(event.eventId) + '\t' + str(event.eventCount))
                print (event.eventStr)

    def log_to_dataframe(self, log_file, regex, headers, logformat):
        
        log_messages = []
        linecount = 0
        with open(log_file, 'r') as fin:
            for line in fin.readlines():
                try:
                    match = regex.search(line.strip())
                    message = [match.group(header) for header in headers]
                    log_messages.append(message)
                    linecount += 1
                except Exception as e:
                    pass
        logdf = pd.DataFrame(log_messages, columns=headers)
        logdf.insert(0, 'LineId', None)
        logdf['LineId'] = [i + 1 for i in range(linecount)]
        return logdf

    def generate_logformat_regex(self, logformat):
        
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''

        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' +', '\\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                regex += '(?P<%s>.*?)' % header
                headers.append(header)

        regex = re.compile('^' + regex + '$')
        return headers, regex

class Partition:
    def __init__(self, stepNo, lenOfLogs, logLL):
        self.stepNo = stepNo
        self.lenOfLogs = lenOfLogs
        self.logLL = logLL
